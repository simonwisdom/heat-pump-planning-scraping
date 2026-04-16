import sys
from pathlib import Path

import pandas as pd

repo_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(repo_root / "scripts" / "pins"))

from pins_06_match_appeals import (  # noqa: E402
    address_signature,
    authority_match,
    canonicalize_authority,
    derive_name_ref,
    normalize_postcode,
    normalize_ref,
    resolve_by_priority,
    tier1_ref_lpa,
    tier2_postcode_ref_substr,
    tier3_postcode_address_signature,
)

# ---- Normalisation helpers -------------------------------------------------


def test_normalize_ref_trims_and_uppercases():
    assert normalize_ref("22/0001/FUL ") == "22/0001/FUL"
    assert normalize_ref("abc") == "ABC"
    assert normalize_ref(None) is None
    assert normalize_ref("") is None


def test_derive_name_ref_takes_tail_after_slash():
    assert derive_name_ref("Authority/22/0001/FUL") == "22/0001/FUL"
    assert derive_name_ref("22/0001/FUL") == "0001/FUL"
    assert derive_name_ref(None) is None


def test_canonicalize_authority_applies_aliases_and_stopwords():
    assert canonicalize_authority("London Borough of Hackney") == "HACKNEY"
    assert canonicalize_authority("Harrogate") == "NORTH YORKSHIRE"  # via alias
    assert canonicalize_authority("St Albans City") == "SAINT ALBANS"


def test_normalize_postcode_handles_formats():
    assert normalize_postcode("SW16 1JY") == "SW16 1JY"
    assert normalize_postcode("sw161jy") == "SW16 1JY"
    assert normalize_postcode("address, LONDON, SW16 1JY (nearest)") == "SW16 1JY"
    assert normalize_postcode(None) is None


def test_authority_match_exact_and_subset():
    assert authority_match("HACKNEY", "HACKNEY").method == "lpa_exact"
    assert authority_match("SOUTH NORFOLK BROADLAND", "SOUTH NORFOLK").method == "lpa_token_subset"
    assert authority_match("HACKNEY", "CAMDEN").matched is False


# ---- Address signature -----------------------------------------------------


def test_address_signature_prefers_house_number():
    assert address_signature("29 Vermont Road, Sutton SM1 3EQ") == "29"
    assert address_signature("Rear Of 45 Etchingham Park Road, London N3 2EB") == "45"


def test_address_signature_skips_noise_words():
    assert address_signature("Land at Station House, Banbury Road") == "STATION"
    assert address_signature("The Cottage, Banbury Road, Charwelton") == "COTTAGE"


def test_address_signature_distinguishes_different_buildings():
    assert address_signature("Charmwood, Greenway, Tatsfield") == "CHARMWOOD"
    assert address_signature("Anhedral House, Greenway, Tatsfield") == "ANHEDRAL"


def test_address_signature_none_when_blank():
    assert address_signature("") is None
    assert address_signature(None) is None


# ---- Tiered matchers -------------------------------------------------------


def _app_row(uid, canon, *, pc="SW1 1AA", addr="29 Example Road, London", desc="", reference=None, name=None):
    ref = reference or uid
    return {
        "uid": uid,
        "source_db": "test",
        "authority_canon": canon,
        "postcode_norm": pc,
        "address_sig": address_signature(addr),
        "uid_ref": uid.upper(),
        "reference_ref": ref.upper(),
        "name_ref": (name or uid).upper(),
        "description_upper": desc.upper(),
    }


def _appeal_row(case, canon, *, pc="SW1 1AA", addr="29 Example Road, London", lpa_ref=None):
    return {
        "case_number_norm": case,
        "authority_canon": canon,
        "postcode_norm": pc,
        "address_sig": address_signature(addr),
        "lpa_app_ref_norm": lpa_ref.upper() if lpa_ref else None,
    }


def test_tier1_matches_on_ref_and_lpa():
    apps = pd.DataFrame([_app_row("22/0001/FUL", "HACKNEY")])
    appeals = pd.DataFrame([_appeal_row("C1", "HACKNEY", lpa_ref="22/0001/FUL")])
    m = tier1_ref_lpa(apps, appeals)
    assert not m.empty
    assert (m["tier"] == "T1_ref_lpa").all()
    assert m[["uid", "case_number"]].drop_duplicates().shape[0] == 1


def test_tier1_requires_lpa_agreement():
    apps = pd.DataFrame([_app_row("22/0001/FUL", "HACKNEY")])
    appeals = pd.DataFrame([_appeal_row("C1", "CAMDEN", lpa_ref="22/0001/FUL")])
    assert tier1_ref_lpa(apps, appeals).empty


def test_tier2_uses_description_when_ref_fields_miss():
    apps = pd.DataFrame(
        [
            _app_row(
                "25/NEW/NMA",
                "WESTMINSTER",
                pc="W1 6DD",
                addr="36 Devonshire Place Mews",
                desc="Amendments to 23/08700/FULL for Demolition of building",
            )
        ]
    )
    appeals = pd.DataFrame(
        [
            _appeal_row(
                "C1",
                "WESTMINSTER",
                pc="W1 6DD",
                addr="36 Devonshire Place Mews",
                lpa_ref="23/08700/FULL",
            )
        ]
    )
    matched = tier2_postcode_ref_substr(apps, appeals)
    assert len(matched) == 1 and matched.iloc[0]["match_detail"].endswith("description")


def test_tier2_rejects_postcode_alone():
    apps = pd.DataFrame([_app_row("A", "HACKNEY", pc="E8 1AA", addr="1 A Rd")])
    appeals = pd.DataFrame([_appeal_row("C1", "HACKNEY", pc="E8 1AA", addr="2 B Rd", lpa_ref="XYZ/ZZZ/QQQ")])
    assert tier2_postcode_ref_substr(apps, appeals).empty


def test_tier3_requires_address_signature_agreement():
    # Same postcode + LPA, different buildings -> reject.
    apps = pd.DataFrame([_app_row("A", "HALTON", pc="WA8 8XW", addr="Roberts Recycling Ltd Pickerings Rd")])
    appeals = pd.DataFrame([_appeal_row("C1", "HALTON", pc="WA8 8XW", addr="Former J Bryan Ltd Pickerings Rd")])
    assert tier3_postcode_address_signature(apps, appeals).empty


def test_tier3_accepts_when_address_signature_agrees():
    apps = pd.DataFrame([_app_row("A", "SUTTON", pc="SM1 3EQ", addr="29 Vermont Road, Sutton")])
    appeals = pd.DataFrame([_appeal_row("C1", "SUTTON", pc="SM1 3EQ", addr="29, Vermont Road, Sutton")])
    matched = tier3_postcode_address_signature(apps, appeals)
    assert len(matched) == 1 and matched.iloc[0]["match_detail"] == "pc_unique+sig=29"


def test_tier3_requires_one_to_one_pair():
    # Two different apps at same (postcode, lpa) -> ambiguous, reject even if one agrees.
    apps = pd.DataFrame(
        [
            _app_row("A", "HACKNEY", pc="E8 1AA", addr="29 Foo Road"),
            _app_row("B", "HACKNEY", pc="E8 1AA", addr="30 Foo Road"),
        ]
    )
    appeals = pd.DataFrame([_appeal_row("C1", "HACKNEY", pc="E8 1AA", addr="29 Foo Road")])
    assert tier3_postcode_address_signature(apps, appeals).empty


def test_resolve_prefers_highest_tier_per_pair():
    hits = pd.DataFrame(
        [
            {
                "uid": "A",
                "source_db": "test",
                "case_number": "C1",
                "tier": "T3_pc_address_sig",
                "match_detail": "pc_unique+sig=29",
            },
            {
                "uid": "A",
                "source_db": "test",
                "case_number": "C1",
                "tier": "T2_pc_ref_substr",
                "match_detail": "pc+ref_substr=uid",
            },
            {
                "uid": "A",
                "source_db": "test",
                "case_number": "C1",
                "tier": "T1_ref_lpa",
                "match_detail": "ref_source=uid",
            },
        ]
    )
    resolved = resolve_by_priority(hits)
    assert len(resolved) == 1 and resolved.iloc[0]["tier"] == "T1_ref_lpa"
