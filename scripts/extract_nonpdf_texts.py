#!/usr/bin/env python3
"""Extract text from the non-PDF, text-bearing documents the PDF pass skipped.

Companion to extract_full_corpus_texts.py, which is PDF-only (its candidate
query filters `LOWER(file_path) LIKE '%.pdf'`). Many councils publish decision
letters and officer/delegated reports as Word/RTF/HTML/email files. Those were
downloaded but never entered the text corpus — silently dropping exactly the
decision-reasoning the downstream LLM extraction depends on.

This pass handles every text-bearing format we can read with the stdlib and
merges rows into the same `full_corpus_texts/summary.csv` manifest, in the
identical 16-column format, so the staging builder and schema pipeline pick
them up:

    .docx .docm .dotx  OOXML Word  -> read word/document.xml, strip tags
    .odt               OpenDocument -> read content.xml, strip tags
    .rtf               Rich Text    -> stdlib RTF control-word stripper
    .html .htm         HTML         -> strip tags
    .txt               plain text   -> read as-is
    .eml               RFC822 email -> stdlib email parser (text/plain pref.)

NOT handled (need an external converter or library, not stdlib):
    .doc .dot          binary Word (OLE)  -> needs libreoffice/antiword
    .msg               Outlook            -> needs extract-msg
They are counted and reported so the remaining gap stays visible. Scanned
images (.tif/.jpg/...) are an OCR-shaped gap shared with the PDF pass and are
out of scope here.

The manifest is rewritten atomically (.tmp + os.replace) and keyed by
document_id, so existing rows are preserved and reruns skip done docs.

Usage on VPS:
    cd /root/heat-pump-planning-scraping
    .venv/bin/python scripts/extract_nonpdf_texts.py \
        --docs-root /mnt/planning-docs \
        --output-dir /root/full_corpus_texts
    # priority subset:
    .venv/bin/python scripts/extract_nonpdf_texts.py ... --uids-file /root/sample50.uids
"""

from __future__ import annotations

import argparse
import csv
import email
import html
import os
import re
import sqlite3
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from email import policy
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_DB = ROOT / "_local/workstreams/01_heat_pump_applications/data/raw/ashp.db"
DEFAULT_DOCS_ROOT = Path("/mnt/planning-docs")
DEFAULT_OUTPUT = Path("/root/full_corpus_texts")

# Extensions handled here, dispatched by extract_text(). Order is irrelevant.
TEXT_EXTS = (".docx", ".docm", ".dotx", ".odt", ".rtf", ".html", ".htm", ".txt", ".eml")
# Reported-but-unhandled, so the remaining gap is visible.
UNHANDLED_EXTS = (".doc", ".dot", ".msg")

# Identical to extract_full_corpus_texts.py so rows interleave cleanly.
SUMMARY_FIELDS = [
    "document_id",
    "application_uid",
    "authority_name",
    "reference",
    "document_type",
    "description",
    "relative_pdf_path",
    "text_path",
    "status",
    "page_count",
    "pages_with_text",
    "word_count",
    "char_count",
    "extractor",
    "error",
    "processed_at",
]


@dataclass(frozen=True)
class Candidate:
    document_id: int
    application_uid: str
    authority_name: str
    reference: str
    document_type: str
    description: str
    relative_path: str


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    p.add_argument("--docs-root", type=Path, default=DEFAULT_DOCS_ROOT)
    p.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    p.add_argument("--limit", type=int, default=None, help="Process at most N candidates")
    p.add_argument("--authority", type=str, default=None)
    p.add_argument(
        "--uids-file", type=Path, default=None, help="Restrict to application_uids listed one-per-line in this file"
    )
    p.add_argument("--checkpoint-every", type=int, default=500)
    p.add_argument("--force", action="store_true", help="Reprocess even if already in manifest")
    return p.parse_args()


def load_uid_filter(path: Path | None) -> set[str] | None:
    if path is None:
        return None
    uids = {ln.strip() for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()}
    return uids


def load_candidates(
    db_path: Path, *, authority: str | None, uids: set[str] | None, limit: int | None
) -> list[Candidate]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        ext_clause = " OR ".join("LOWER(d.file_path) LIKE ?" for _ in TEXT_EXTS)
        sql = f"""
            SELECT
                d.id AS document_id,
                d.application_uid,
                a.authority_name,
                a.reference,
                d.document_type,
                d.description,
                d.file_path
            FROM documents d
            JOIN applications a ON a.uid = d.application_uid
            WHERE d.download_status = 'downloaded'
              AND ({ext_clause})
              AND trim(COALESCE(d.file_path, '')) <> ''
        """
        params: list[str] = [f"%{e}" for e in TEXT_EXTS]
        if authority:
            sql += " AND a.authority_name = ?"
            params.append(authority)
        sql += " ORDER BY a.authority_name, a.reference, d.id"
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()

    out: list[Candidate] = []
    for row in rows:
        rel = (row["file_path"] or "").strip().lstrip("/")
        if not rel:
            continue
        if uids is not None and row["application_uid"] not in uids:
            continue
        out.append(
            Candidate(
                document_id=row["document_id"],
                application_uid=row["application_uid"],
                authority_name=row["authority_name"] or "",
                reference=row["reference"] or "",
                document_type=row["document_type"] or "",
                description=row["description"] or "",
                relative_path=rel,
            )
        )
    if limit is not None:
        out = out[:limit]
    return out


def count_unhandled(db_path: Path) -> dict[str, int]:
    conn = sqlite3.connect(str(db_path))
    try:
        out: dict[str, int] = {}
        for ext in UNHANDLED_EXTS:
            (n,) = conn.execute(
                "SELECT COUNT(*) FROM documents WHERE download_status='downloaded' AND LOWER(file_path) LIKE ?",
                (f"%{ext}",),
            ).fetchone()
            out[ext] = int(n)
    finally:
        conn.close()
    return out


def load_existing(summary_path: Path) -> dict[int, dict[str, str]]:
    if not summary_path.exists():
        return {}
    csv.field_size_limit(1 << 30)
    out: dict[int, dict[str, str]] = {}
    with summary_path.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            try:
                key = int(row.get("document_id", ""))
            except ValueError:
                continue
            out[key] = {f: row.get(f, "") for f in SUMMARY_FIELDS}
    return out


# --- whitespace normalisation shared by all extractors -----------------------
def _clean(text: str) -> str:
    text = html.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{2,}", "\n", text)
    return text.strip()


def extract_ooxml_word(path: Path) -> str:
    """docx/docm/dotx: text lives in word/document.xml; tables are nested <w:p>."""
    with zipfile.ZipFile(path) as z:
        if "word/document.xml" not in z.namelist():
            return ""
        xml = z.read("word/document.xml").decode("utf-8", "replace")
    xml = re.sub(r"<w:tab\b[^>]*/>", "\t", xml)
    xml = re.sub(r"</w:p>", "\n", xml)
    xml = re.sub(r"<w:br\b[^>]*/>", "\n", xml)
    return _clean(re.sub(r"<[^>]+>", "", xml))


def extract_odt(path: Path) -> str:
    """ODF text: content.xml, paragraphs are <text:p>/<text:h>."""
    with zipfile.ZipFile(path) as z:
        if "content.xml" not in z.namelist():
            return ""
        xml = z.read("content.xml").decode("utf-8", "replace")
    xml = re.sub(r"<text:tab\b[^>]*/>", "\t", xml)
    xml = re.sub(r"<text:line-break\b[^>]*/>", "\n", xml)
    xml = re.sub(r"</text:(p|h)>", "\n", xml)
    return _clean(re.sub(r"<[^>]+>", "", xml))


def extract_html(path: Path) -> str:
    raw = path.read_text(encoding="utf-8", errors="replace")
    raw = re.sub(r"(?is)<(script|style)\b.*?</\1>", " ", raw)
    raw = re.sub(r"(?i)<br\s*/?>", "\n", raw)
    raw = re.sub(r"(?i)</(p|div|li|tr|h[1-6])>", "\n", raw)
    return _clean(re.sub(r"<[^>]+>", " ", raw))


def extract_txt(path: Path) -> str:
    return _clean(path.read_text(encoding="utf-8", errors="replace"))


def extract_eml(path: Path) -> str:
    msg = email.message_from_bytes(path.read_bytes(), policy=policy.default)
    body = msg.get_body(preferencelist=("plain", "html"))
    content = ""
    if body is not None:
        content = body.get_content()
        if body.get_content_type() == "text/html":
            content = re.sub(r"<[^>]+>", " ", content)
    header = f"Subject: {msg.get('subject', '')}\nFrom: {msg.get('from', '')}\n\n"
    return _clean(header + content)


# RFC1924-style RTF text extractor (stdlib). Adapted from the well-known
# pure-python "striprtf" algorithm: walk control words, hex/unicode escapes and
# group nesting, dropping ignorable destination groups (fonttbl, colortbl, etc.).
_RTF_PATTERN = re.compile(
    r"\\([a-z]{1,32})(-?\d{1,10})?[ ]?|\\'([0-9a-f]{2})|\\([^a-z])|([{}])|[\r\n]+|(.)",
    re.IGNORECASE,
)
_RTF_DESTINATIONS = frozenset(
    (
        "aftncn",
        "aftnsep",
        "aftnsepc",
        "annotation",
        "atnauthor",
        "atndate",
        "atnicn",
        "atnid",
        "atnparent",
        "atnref",
        "atntime",
        "atrfend",
        "atrfstart",
        "author",
        "background",
        "bkmkend",
        "bkmkstart",
        "blipuid",
        "buptim",
        "category",
        "colorschememapping",
        "colortbl",
        "comment",
        "company",
        "creatim",
        "datafield",
        "datastore",
        "defchp",
        "defpap",
        "do",
        "doccomm",
        "docvar",
        "dptxbxtext",
        "ebcend",
        "ebcstart",
        "factoidname",
        "falt",
        "fchars",
        "ffdeftext",
        "ffentrymcr",
        "ffexitmcr",
        "ffformat",
        "ffhelptext",
        "ffl",
        "ffname",
        "ffstattext",
        "field",
        "file",
        "filetbl",
        "fldinst",
        "fldrslt",
        "fldtype",
        "fname",
        "fontemb",
        "fontfile",
        "fonttbl",
        "footer",
        "footerf",
        "footerl",
        "footerr",
        "footnote",
        "formfield",
        "ftncn",
        "ftnsep",
        "ftnsepc",
        "g",
        "generator",
        "gridtbl",
        "header",
        "headerf",
        "headerl",
        "headerr",
        "hl",
        "hlfr",
        "hlinkbase",
        "hlloc",
        "hlsrc",
        "hsv",
        "htmltag",
        "info",
        "keycode",
        "keywords",
        "latentstyles",
        "lchars",
        "levelnumbers",
        "leveltext",
        "lfolevel",
        "linkval",
        "list",
        "listlevel",
        "listname",
        "listoverride",
        "listoverridetable",
        "listpicture",
        "liststylename",
        "listtable",
        "listtext",
        "lsdlockedexcept",
        "macc",
        "maccPr",
        "mailmerge",
        "maln",
        "malnScr",
        "manager",
        "margPr",
        "mbar",
        "mbarPr",
        "mbaseJc",
        "mbegChr",
        "mborderBox",
        "mborderBoxPr",
        "mbox",
        "mboxPr",
        "mchr",
        "mcount",
        "mctrlPr",
        "md",
        "mdeg",
        "mdegHide",
        "mden",
        "mdiff",
        "mdPr",
        "me",
        "mendChr",
        "meqArr",
        "meqArrPr",
        "mf",
        "mfName",
        "mfPr",
        "mfunc",
        "mfuncPr",
        "mgroupChr",
        "mgroupChrPr",
        "mgrow",
        "mhideBot",
        "mhideLeft",
        "mhideRight",
        "mhideTop",
        "mhtmltag",
        "mlim",
        "mlimloc",
        "mlimlow",
        "mlimlowPr",
        "mlimupp",
        "mlimuppPr",
        "mm",
        "mmaddfieldname",
        "mmath",
        "mmathPict",
        "mmathPr",
        "mmaxdist",
        "mmc",
        "mmcJc",
        "mmconnectstr",
        "mmconnectstrdata",
        "mmcPr",
        "mmcs",
        "mmdatasource",
        "mmheadersource",
        "mmmailsubject",
        "mmodso",
        "mmodsofilter",
        "mmodsofldmpdata",
        "mmodsomappedname",
        "mmodsoname",
        "mmodsorecipdata",
        "mmodsosort",
        "mmodsosrc",
        "mmodsotable",
        "mmodsoudl",
        "mmodsoudldata",
        "mmodsouniquetag",
        "mmPr",
        "mmquery",
        "mmr",
        "mnary",
        "mnaryPr",
        "mnoBreak",
        "mnum",
        "mobjDist",
        "moMath",
        "moMathPara",
        "moMathParaPr",
        "mopEmu",
        "mphant",
        "mphantPr",
        "mplcHide",
        "mpos",
        "mr",
        "mrad",
        "mradPr",
        "mrPr",
        "msepChr",
        "mshow",
        "mshp",
        "msPre",
        "msPrePr",
        "msSub",
        "msSubPr",
        "msSubSup",
        "msSubSupPr",
        "msSup",
        "msSupPr",
        "mstrikeBLTR",
        "mstrikeH",
        "mstrikeTLBR",
        "mstrikeV",
        "msub",
        "msubHide",
        "msup",
        "msupHide",
        "mtransp",
        "mtype",
        "mvertJc",
        "mvfmf",
        "mvfml",
        "mvtof",
        "mvtol",
        "mzeroAsc",
        "mzeroDesc",
        "mzeroWid",
        "nesttableprops",
        "nextfile",
        "nonesttables",
        "objalias",
        "objclass",
        "objdata",
        "object",
        "objname",
        "objsect",
        "objtime",
        "oldcprops",
        "oldpprops",
        "oldsprops",
        "oldtprops",
        "oleclsid",
        "operator",
        "panose",
        "password",
        "passwordhash",
        "pgp",
        "pgptbl",
        "picprop",
        "pict",
        "pn",
        "pnseclvl",
        "pntext",
        "pntxta",
        "pntxtb",
        "printim",
        "private",
        "propname",
        "protend",
        "protstart",
        "protusertbl",
        "pxe",
        "result",
        "revtbl",
        "revtim",
        "rsidtbl",
        "rxe",
        "shp",
        "shpgrp",
        "shpinst",
        "shppict",
        "shprslt",
        "shptxt",
        "sn",
        "sp",
        "staticval",
        "stylesheet",
        "subject",
        "sv",
        "svb",
        "tc",
        "template",
        "themedata",
        "title",
        "txe",
        "ud",
        "upr",
        "userprops",
        "wgrffmtfilter",
        "windowcaption",
        "writereservation",
        "writereservhash",
        "xe",
        "xform",
        "xmlattrname",
        "xmlattrvalue",
        "xmlclose",
        "xmlname",
        "xmlnstbl",
        "xmlopen",
    )
)
_RTF_SPECIAL = {
    "par": "\n",
    "sect": "\n\n",
    "page": "\n\n",
    "line": "\n",
    "tab": "\t",
    "emdash": "—",
    "endash": "–",
    "emspace": " ",
    "enspace": " ",
    "qmspace": " ",
    "bullet": "•",
    "lquote": "‘",
    "rquote": "’",
    "ldblquote": "“",
    "rdblquote": "”",
}


def _strip_rtf(text: str) -> str:
    stack: list[tuple[int, bool]] = []
    ignorable = False
    ucskip = 1
    curskip = 0
    out: list[str] = []
    for match in _RTF_PATTERN.finditer(text):
        word, arg, hexcode, char, brace, tchar = match.groups()
        if brace:
            curskip = 0
            if brace == "{":
                stack.append((ucskip, ignorable))
            elif brace == "}" and stack:
                ucskip, ignorable = stack.pop()
        elif char:
            curskip = 0
            if char == "~":
                if not ignorable:
                    out.append(" ")
            elif char in "{}\\":
                if not ignorable:
                    out.append(char)
            elif char == "*":
                ignorable = True
        elif word:
            curskip = 0
            if word in _RTF_DESTINATIONS:
                ignorable = True
            elif ignorable:
                pass
            elif word in _RTF_SPECIAL:
                out.append(_RTF_SPECIAL[word])
            elif word == "uc":
                ucskip = int(arg) if arg else 1
            elif word == "u":
                c = int(arg)
                if c < 0:
                    c += 0x10000
                out.append(chr(c) if c <= 0x10FFFF else "?")
                curskip = ucskip
        elif hexcode:
            if curskip > 0:
                curskip -= 1
            elif not ignorable:
                # \'xx is a codepage byte (Word emits Windows-1252), not a
                # Unicode codepoint, so decode it as cp1252: \'92 -> ’, \'97 -> —.
                out.append(bytes([int(hexcode, 16)]).decode("cp1252", "replace"))
        elif tchar:
            if curskip > 0:
                curskip -= 1
            elif not ignorable:
                out.append(tchar)
    return "".join(out)


def extract_rtf(path: Path) -> str:
    raw = path.read_text(encoding="latin-1", errors="replace")
    return _clean(_strip_rtf(raw))


def extract_text(path: Path) -> tuple[str, str]:
    """Dispatch by extension. Returns (text, extractor_tag)."""
    ext = path.suffix.lower()
    if ext in (".docx", ".docm", ".dotx"):
        return extract_ooxml_word(path), "ooxml-word"
    if ext == ".odt":
        return extract_odt(path), "odt-xml"
    if ext == ".rtf":
        return extract_rtf(path), "rtf-strip"
    if ext in (".html", ".htm"):
        return extract_html(path), "html-strip"
    if ext == ".txt":
        return extract_txt(path), "txt"
    if ext == ".eml":
        return extract_eml(path), "eml"
    return "", "skip"


def extract_one(cand: Candidate, docs_root: Path, output_dir: Path) -> dict[str, object]:
    src = docs_root / cand.relative_path
    text_rel = Path("texts") / Path(cand.relative_path).with_suffix(".txt")
    text_abs = output_dir / text_rel

    row: dict[str, object] = {
        "document_id": cand.document_id,
        "application_uid": cand.application_uid,
        "authority_name": cand.authority_name,
        "reference": cand.reference,
        "document_type": cand.document_type,
        "description": cand.description,
        "relative_pdf_path": cand.relative_path,
        "text_path": "",
        "status": "",
        "page_count": 0,
        "pages_with_text": 0,
        "word_count": 0,
        "char_count": 0,
        "extractor": "",
        "error": "",
        "processed_at": datetime.now(timezone.utc).isoformat(),
    }

    if not src.exists():
        row["status"] = "missing_file"
        row["error"] = f"not found at {src}"
        return row

    try:
        text, tag = extract_text(src)
    except zipfile.BadZipFile as exc:
        row["status"] = "extract_error"
        row["error"] = f"bad_zip: {exc!r}"[:500]
        return row
    except Exception as exc:  # broad: keep the sweep going
        row["status"] = "extract_error"
        row["error"] = f"{exc!r}"[:500]
        return row

    row["extractor"] = tag
    row["char_count"] = len(text)
    row["word_count"] = len(text.split())
    if not text.strip():
        row["status"] = "no_text"
        return row

    text_abs.parent.mkdir(parents=True, exist_ok=True)
    try:
        text_abs.write_text(text, encoding="utf-8")
    except OSError as exc:
        row["status"] = "extract_error"
        row["error"] = f"write: {exc!r}"[:500]
        return row

    row["text_path"] = str(text_rel)
    row["status"] = "extracted"
    return row


def write_summary(summary_path: Path, rows_by_id: dict[int, dict[str, object]]) -> None:
    tmp = summary_path.with_suffix(".csv.tmp")
    ordered = sorted(rows_by_id.values(), key=lambda r: int(r.get("document_id", 0)))
    with tmp.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=SUMMARY_FIELDS)
        w.writeheader()
        for r in ordered:
            w.writerow({k: r.get(k, "") for k in SUMMARY_FIELDS})
    os.replace(tmp, summary_path)


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = args.output_dir / "summary.csv"

    print(f"DB:         {args.db_path}", flush=True)
    print(f"Docs root:  {args.docs_root}", flush=True)
    print(f"Output dir: {args.output_dir}", flush=True)

    uids = load_uid_filter(args.uids_file)
    if uids is not None:
        print(f"UID filter: {len(uids)} uids from {args.uids_file}", flush=True)

    candidates = load_candidates(args.db_path, authority=args.authority, uids=uids, limit=args.limit)
    print(f"Candidates: {len(candidates)}", flush=True)
    gap = count_unhandled(args.db_path)
    gap_str = ", ".join(f"{ext}={n}" for ext, n in gap.items() if n)
    if gap_str:
        print(f"NOTE: still-unhandled (need converter/lib, not stdlib): {gap_str}", flush=True)

    rows_by_id = {} if args.force else load_existing(summary_path)
    print(f"Existing manifest rows: {len(rows_by_id)}", flush=True)

    skip = set()
    if not args.force:
        for c in candidates:
            r = rows_by_id.get(c.document_id)
            if r and r.get("status") == "extracted":
                tp = r.get("text_path", "")
                if tp and (args.output_dir / tp).exists():
                    skip.add(c.document_id)
    todo = [c for c in candidates if c.document_id not in skip]
    print(f"Already extracted (skip): {len(skip)}   To process: {len(todo)}", flush=True)
    if not todo:
        print("Nothing to do.", flush=True)
        return

    t0 = time.time()
    n_ok = n_empty = n_missing = n_err = 0
    for i, cand in enumerate(todo, 1):
        row = extract_one(cand, args.docs_root, args.output_dir)
        rows_by_id[int(row["document_id"])] = row
        status = row.get("status")
        n_ok += status == "extracted"
        n_empty += status == "no_text"
        n_missing += status == "missing_file"
        n_err += status == "extract_error"
        if i % 50 == 0 or i == len(todo):
            rate = i / (time.time() - t0) if time.time() > t0 else 0
            print(
                f"[{i}/{len(todo)}] ok={n_ok} empty={n_empty} miss={n_missing} err={n_err} rate={rate:.1f}/s",
                flush=True,
            )
        if i % args.checkpoint_every == 0:
            write_summary(summary_path, rows_by_id)

    write_summary(summary_path, rows_by_id)
    print("\n=== Done ===", flush=True)
    print(
        f"Processed {len(todo)}: ok={n_ok} empty={n_empty} miss={n_missing} err={n_err} in {(time.time() - t0):.1f}s",
        flush=True,
    )
    print(f"Summary: {summary_path}", flush=True)


if __name__ == "__main__":
    main()
