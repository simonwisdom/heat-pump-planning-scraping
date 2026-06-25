"""Batch-API runner for the v4.18 schema extraction over the full corpus.

Reuses build_request_body() / parse_result() from extract_schema_v1 so the
batched prompt is byte-identical to the validated live prompt. Submits the
corpus as size-chunked OpenAI Batch jobs (50% cheaper, 24h turnaround) and is
resumable at every step — a dropped connection mid-run loses nothing.

Pipeline (all state lives under a run dir, --run-dir / $HP_RUN_DIR):

    build     sample.csv + selection.json -> requests/chunk_*.jsonl + used.json
              + manifest.json   (no API calls; safe to inspect before submitting)
    submit    upload each chunk, create a Batch, record ids in manifest
    poll      print per-batch status (one line each)
    collect   download finished batches, parse -> results.json/csv,
              record done_uids + failed_uids
    retry     re-submit failed_uids as ONE more batch (the documented single retry)
    fill      live-fill any still-failing uids via extract_one() (synchronous)
    status    summarise the manifest

Decisions (2026-06-25): on failure, `retry` once as a batch, then `fill` the
residual live — that guarantees 100% coverage. Rollout: build, submit ONE chunk
(`--only chunk_000`), collect + QA, then submit the rest.

Run where the staging texts live (the VPS). Staging is resolved via
HP_SAMPLE_ROOT, exactly as extract_schema_v1 resolves it; HP_MODEL /
HP_REASONING_EFFORT carry through identically so batch == live config.

    HP_SAMPLE_ROOT=/root/full_staging \
        python scripts/llm/batch_extract.py build  --run-dir /root/corpus_run
    ... submit --only chunk_000 ... collect ... submit ... collect ... retry ... fill
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from pathlib import Path

# Default per-chunk request-file cap. OpenAI's Batch input-file limit is 200 MB;
# we leave headroom. At ~100 KB/request this is ~1,500-1,800 apps/chunk, so the
# full corpus is ~12-14 chunks (well under the 50k-requests/file limit too).
DEFAULT_MAX_CHUNK_BYTES = 150 * 1024 * 1024

BATCH_URL = "/v1/chat/completions"

# csv field that build_request_body / the flat CSV need from each staging row.
ROW_FIELDS = (
    "uid",
    "authority_name",
    "reference",
    "description",
    "planning_decision",
    "decision_date",
    "decision_bucket",
    "final_label",
)


# --------------------------------------------------------------------------- #
# Pure helpers (no openai / no extract_schema_v1 import — unit-tested directly)
# --------------------------------------------------------------------------- #
def jsonl_request(uid: str, body: dict) -> dict:
    """One Batch input line: a POST to the chat-completions endpoint."""
    return {"custom_id": uid, "method": "POST", "url": BATCH_URL, "body": body}


def pack_chunks(line_lengths: list[int], max_bytes: int) -> list[list[int]]:
    """Greedily group request indices so each group's bytes <= max_bytes.

    A single request larger than max_bytes gets its own chunk (it can't be
    split); callers should warn on that case. Order is preserved.
    """
    chunks: list[list[int]] = []
    cur: list[int] = []
    cur_bytes = 0
    for i, n in enumerate(line_lengths):
        if cur and cur_bytes + n > max_bytes:
            chunks.append(cur)
            cur, cur_bytes = [], 0
        cur.append(i)
        cur_bytes += n
    if cur:
        chunks.append(cur)
    return chunks


def pending_uids(all_uids: list[str], done_uids: set[str]) -> list[str]:
    """Uids not yet collected, order-preserving — drives resume."""
    return [u for u in all_uids if u not in done_uids]


# --------------------------------------------------------------------------- #
# Run-dir / manifest plumbing
# --------------------------------------------------------------------------- #
def _ex():
    """Lazy import: keep the heavy module (openai) out of pure-helper imports."""
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import extract_schema_v1 as ex  # noqa: E402

    return ex


def load_manifest(run_dir: Path) -> dict:
    path = run_dir / "manifest.json"
    if path.exists():
        return json.loads(path.read_text())
    return {"chunks": [], "batches": {}, "done_uids": [], "failed_uids": []}


def save_manifest(run_dir: Path, manifest: dict) -> None:
    (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))


def read_rows(ex) -> list[dict]:
    return list(csv.DictReader(ex.SAMPLE_CSV.open(encoding="utf-8")))


# --------------------------------------------------------------------------- #
# build
# --------------------------------------------------------------------------- #
def cmd_build(args) -> int:
    run_dir = Path(args.run_dir)
    req_dir = run_dir / "requests"
    req_dir.mkdir(parents=True, exist_ok=True)

    ex = _ex()
    rows = read_rows(ex)
    uid_files = ex.load_selection()
    print(f"Building requests for {len(rows)} apps from {ex.SAMPLE_CSV}", flush=True)

    lines: list[str] = []
    used_map: dict[str, list[dict]] = {}
    for r in rows:
        body, used = ex.build_request_body(r, uid_files)
        lines.append(json.dumps(jsonl_request(r["uid"], body)))
        used_map[r["uid"]] = used

    lengths = [len(s.encode("utf-8")) + 1 for s in lines]  # +1 for newline
    oversize = [i for i, n in enumerate(lengths) if n > args.max_chunk_bytes]
    for i in oversize:
        print(
            f"  WARNING: request {rows[i]['uid']} is {lengths[i]:,} B > chunk cap "
            f"{args.max_chunk_bytes:,} B; it gets its own chunk.",
            flush=True,
        )

    groups = pack_chunks(lengths, args.max_chunk_bytes)
    chunks_meta = []
    for ci, idxs in enumerate(groups):
        name = f"chunk_{ci:03d}"
        (req_dir / f"{name}.jsonl").write_text("\n".join(lines[i] for i in idxs) + "\n")
        nbytes = sum(lengths[i] for i in idxs)
        chunks_meta.append({"name": name, "n": len(idxs), "bytes": nbytes})
        print(f"  {name}: {len(idxs)} apps, {nbytes / 1e6:.1f} MB", flush=True)

    (run_dir / "used.json").write_text(json.dumps(used_map))

    manifest = load_manifest(run_dir)
    manifest.update(
        {
            "staging_dir": str(ex.SAMPLE_ROOT),
            "model": ex.MODEL,
            "reasoning_effort": os.environ.get("HP_REASONING_EFFORT", "low"),
            "max_chunk_bytes": args.max_chunk_bytes,
            "n_apps": len(rows),
            "chunks": chunks_meta,
        }
    )
    manifest.setdefault("batches", {})
    manifest.setdefault("done_uids", [])
    manifest.setdefault("failed_uids", [])
    save_manifest(run_dir, manifest)

    total_mb = sum(c["bytes"] for c in chunks_meta) / 1e6
    print(f"\n{len(chunks_meta)} chunks, {total_mb:.1f} MB total -> {run_dir}", flush=True)
    print("Next: submit (add --only chunk_000 to pilot a single chunk first).", flush=True)
    return 0


# --------------------------------------------------------------------------- #
# submit
# --------------------------------------------------------------------------- #
def cmd_submit(args) -> int:
    run_dir = Path(args.run_dir)
    ex = _ex()
    ex.load_dotenv(ex.ROOT / ".env")
    if not os.environ.get("OPENAI_API_KEY"):
        print("OPENAI_API_KEY not in env", file=sys.stderr)
        return 1
    client = ex.OpenAI()
    manifest = load_manifest(run_dir)

    targets = [c["name"] for c in manifest["chunks"]]
    if args.only:
        targets = [c for c in targets if c in set(args.only)]
    submitted = 0
    for name in targets:
        b = manifest["batches"].get(name)
        if b and b.get("id") and b.get("status") not in ("failed", "expired", "cancelled"):
            print(f"  {name}: already submitted ({b['id']}, {b.get('status')}), skipping", flush=True)
            continue
        path = run_dir / "requests" / f"{name}.jsonl"
        print(f"  {name}: uploading {path.name} ...", flush=True)
        up = client.files.create(file=path.open("rb"), purpose="batch")
        batch = client.batches.create(
            input_file_id=up.id,
            endpoint=BATCH_URL,
            completion_window="24h",
            metadata={"run": run_dir.name, "chunk": name, "kind": "main"},
        )
        manifest["batches"][name] = {
            "id": batch.id,
            "input_file_id": up.id,
            "status": batch.status,
            "output_file_id": None,
            "error_file_id": None,
            "kind": "main",
        }
        save_manifest(run_dir, manifest)
        submitted += 1
        print(f"    -> {batch.id} ({batch.status})", flush=True)
    print(f"Submitted {submitted} batch(es).", flush=True)
    return 0


# --------------------------------------------------------------------------- #
# poll
# --------------------------------------------------------------------------- #
def cmd_poll(args) -> int:
    run_dir = Path(args.run_dir)
    ex = _ex()
    ex.load_dotenv(ex.ROOT / ".env")
    client = ex.OpenAI()
    manifest = load_manifest(run_dir)

    n_done = 0
    for name, b in manifest["batches"].items():
        batch = client.batches.retrieve(b["id"])
        b["status"] = batch.status
        b["output_file_id"] = batch.output_file_id
        b["error_file_id"] = batch.error_file_id
        rc = batch.request_counts
        print(
            f"  {name}: {batch.status}  ({rc.completed}/{rc.total} done, {rc.failed} failed)",
            flush=True,
        )
        if batch.status == "completed":
            n_done += 1
    save_manifest(run_dir, manifest)
    print(f"{n_done}/{len(manifest['batches'])} batches completed.", flush=True)
    return 0


# --------------------------------------------------------------------------- #
# collect
# --------------------------------------------------------------------------- #
def _flat_fields(ex) -> list[str]:
    return (
        ["uid", "authority_name", "reference", "decision_bucket", "final_label_recipe_b"]
        + list(ex.T0.keys())
        + list(ex.T1.keys())
    )


def _assemble(ex, out: dict, row: dict) -> dict:
    """Attach the same metadata columns main() adds to each live result."""
    out["uid"] = row["uid"]
    out["reference"] = row["reference"]
    out["authority_name"] = row["authority_name"]
    out["planning_decision"] = row["planning_decision"]
    out["decision_bucket"] = row["decision_bucket"]
    out["description"] = row["description"]
    out["final_label_recipe_b"] = row.get("final_label", "")
    return out


def _write_results(ex, run_dir: Path, by_uid: dict[str, dict]) -> None:
    results = [by_uid[u] for u in sorted(by_uid)]
    (run_dir / "results.json").write_text(json.dumps(results, indent=2))
    flat = _flat_fields(ex)
    with (run_dir / "results.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=flat)
        w.writeheader()
        for r in results:
            row = {k: r.get(k, "") for k in flat}
            for k, v in row.items():
                if isinstance(v, list):
                    row[k] = "|".join(str(x) for x in v)
            w.writerow(row)


def _load_results(run_dir: Path) -> dict[str, dict]:
    path = run_dir / "results.json"
    if not path.exists():
        return {}
    return {r["uid"]: r for r in json.loads(path.read_text())}


def cmd_collect(args) -> int:
    run_dir = Path(args.run_dir)
    ex = _ex()
    ex.load_dotenv(ex.ROOT / ".env")
    client = ex.OpenAI()
    manifest = load_manifest(run_dir)

    rows_by_uid = {r["uid"]: r for r in read_rows(ex)}
    used_map = json.loads((run_dir / "used.json").read_text())
    by_uid = _load_results(run_dir)
    done = set(manifest.get("done_uids", []))
    failed = set(manifest.get("failed_uids", []))

    for name, b in manifest["batches"].items():
        batch = client.batches.retrieve(b["id"])
        b["status"] = batch.status
        b["output_file_id"] = batch.output_file_id
        b["error_file_id"] = batch.error_file_id
        if batch.status != "completed" or not batch.output_file_id:
            print(f"  {name}: {batch.status}, not collectable yet", flush=True)
            continue
        text = client.files.content(batch.output_file_id).text
        n_ok = n_err = 0
        for line in text.splitlines():
            if not line.strip():
                continue
            rec = json.loads(line)
            uid = rec["custom_id"]
            resp = rec.get("response") or {}
            err = rec.get("error")
            if err or resp.get("status_code") != 200:
                failed.add(uid)
                n_err += 1
                continue
            try:
                body = resp["body"]
                content = body["choices"][0]["message"]["content"]
                out = ex.parse_result(content, used_map.get(uid, []))
                usage = body.get("usage") or {}
                out["_usage"] = {
                    "prompt_tokens": usage.get("prompt_tokens", 0),
                    "completion_tokens": usage.get("completion_tokens", 0),
                }
                _assemble(ex, out, rows_by_uid[uid])
                by_uid[uid] = out
                done.add(uid)
                failed.discard(uid)
                n_ok += 1
            except Exception as exc:  # parse/normalise failure -> live-fill later
                failed.add(uid)
                n_err += 1
                print(f"    parse error {uid}: {exc}", flush=True)
        print(f"  {name}: collected {n_ok} ok, {n_err} failed", flush=True)

    manifest["done_uids"] = sorted(done)
    manifest["failed_uids"] = sorted(failed - done)
    save_manifest(run_dir, manifest)
    _write_results(ex, run_dir, by_uid)
    print(
        f"Totals: {len(done)} done, {len(manifest['failed_uids'])} failed -> {run_dir}/results.json",
        flush=True,
    )
    return 0


# --------------------------------------------------------------------------- #
# retry (one extra batch of just the failures) + fill (live)
# --------------------------------------------------------------------------- #
def cmd_retry(args) -> int:
    run_dir = Path(args.run_dir)
    ex = _ex()
    ex.load_dotenv(ex.ROOT / ".env")
    if not os.environ.get("OPENAI_API_KEY"):
        print("OPENAI_API_KEY not in env", file=sys.stderr)
        return 1
    client = ex.OpenAI()
    manifest = load_manifest(run_dir)
    failed = manifest.get("failed_uids", [])
    if not failed:
        print("No failed uids to retry.", flush=True)
        return 0
    if "retry" in manifest["batches"]:
        print("Retry batch already exists; collect it instead of re-retrying.", flush=True)
        return 0

    rows_by_uid = {r["uid"]: r for r in read_rows(ex)}
    uid_files = ex.load_selection()
    path = run_dir / "requests" / "retry.jsonl"
    with path.open("w") as fh:
        for uid in failed:
            body, _ = ex.build_request_body(rows_by_uid[uid], uid_files)
            fh.write(json.dumps(jsonl_request(uid, body)) + "\n")
    print(f"Retry chunk: {len(failed)} apps -> {path.name}", flush=True)
    up = client.files.create(file=path.open("rb"), purpose="batch")
    batch = client.batches.create(
        input_file_id=up.id,
        endpoint=BATCH_URL,
        completion_window="24h",
        metadata={"run": run_dir.name, "chunk": "retry", "kind": "retry"},
    )
    manifest["batches"]["retry"] = {
        "id": batch.id,
        "input_file_id": up.id,
        "status": batch.status,
        "output_file_id": None,
        "error_file_id": None,
        "kind": "retry",
    }
    save_manifest(run_dir, manifest)
    print(f"  -> {batch.id} ({batch.status}). Poll then collect.", flush=True)
    return 0


def cmd_fill(args) -> int:
    run_dir = Path(args.run_dir)
    ex = _ex()
    ex.load_dotenv(ex.ROOT / ".env")
    if not os.environ.get("OPENAI_API_KEY"):
        print("OPENAI_API_KEY not in env", file=sys.stderr)
        return 1
    client = ex.OpenAI()
    manifest = load_manifest(run_dir)
    failed = list(manifest.get("failed_uids", []))
    if not failed:
        print("No residual failures to fill.", flush=True)
        return 0

    rows_by_uid = {r["uid"]: r for r in read_rows(ex)}
    uid_files = ex.load_selection()
    by_uid = _load_results(run_dir)
    done = set(manifest.get("done_uids", []))
    still_failed = []
    print(f"Live-filling {len(failed)} residual app(s)...", flush=True)
    for uid in failed:
        try:
            out = ex.extract_one(client, rows_by_uid[uid], uid_files)
            _assemble(ex, out, rows_by_uid[uid])
            by_uid[uid] = out
            done.add(uid)
            print(f"  filled {uid}", flush=True)
        except Exception as exc:
            still_failed.append(uid)
            print(f"  STILL FAILED {uid}: {exc}", flush=True)

    manifest["done_uids"] = sorted(done)
    manifest["failed_uids"] = sorted(still_failed)
    save_manifest(run_dir, manifest)
    _write_results(ex, run_dir, by_uid)
    print(f"Filled {len(failed) - len(still_failed)}; {len(still_failed)} still failed.", flush=True)
    return 0


def cmd_status(args) -> int:
    run_dir = Path(args.run_dir)
    manifest = load_manifest(run_dir)
    n_apps = manifest.get("n_apps", "?")
    print(f"Run: {run_dir}  ({manifest.get('model')}, effort={manifest.get('reasoning_effort')})")
    print(f"  apps: {n_apps}  chunks: {len(manifest.get('chunks', []))}")
    for name, b in manifest.get("batches", {}).items():
        print(f"  batch {name}: {b.get('id')}  {b.get('status')}  ({b.get('kind')})")
    print(f"  done: {len(manifest.get('done_uids', []))}  failed: {len(manifest.get('failed_uids', []))}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--run-dir", default=os.environ.get("HP_RUN_DIR", "_local/llm_pilot/corpus_run"))
    sub = p.add_subparsers(dest="cmd", required=True)

    pb = sub.add_parser("build", help="build chunked JSONL requests + manifest (no API calls)")
    pb.add_argument("--max-chunk-bytes", type=int, default=DEFAULT_MAX_CHUNK_BYTES)
    pb.set_defaults(func=cmd_build)

    ps = sub.add_parser("submit", help="upload chunks and create batches")
    ps.add_argument("--only", nargs="*", help="submit only these chunk names (e.g. chunk_000)")
    ps.set_defaults(func=cmd_submit)

    for name, fn, helptext in [
        ("poll", cmd_poll, "print per-batch status"),
        ("collect", cmd_collect, "download finished batches -> results.json/csv"),
        ("retry", cmd_retry, "re-submit failed uids as one more batch"),
        ("fill", cmd_fill, "live-fill residual failures via extract_one"),
        ("status", cmd_status, "summarise the manifest"),
    ]:
        sp = sub.add_parser(name, help=helptext)
        sp.set_defaults(func=fn)

    args = p.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
