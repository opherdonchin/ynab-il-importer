import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ynab_il_importer.io_bankin import extract_merchant


@pytest.mark.parametrize(
    ("description_raw", "expected_kind", "expected_merchant"),
    [
        (
            "ב-מאפית אורן משי באר שבע0740- בכרטיס המסתיים ב20:04  19/01/26 כרטיס דביט מתאריך",
            "debit_card",
            "מאפית אורן משי באר שבע",
        ),
        (
            "LIME*RIDE JBY5         WWW.LI.ME     US- ב0849- בכרטיס המסתיים ב19:22  31/01/26 כרטיס דביט מתאריך",
            "debit_card",
            "LIME*RIDE JBY5 WWW.LI.ME US",
        ),
        (
            "BIT- ב0849- בכרטיס המסתיים ב13:45  19/01/26 כרטיס דביט מתאריך",
            "bit",
            "BIT",
        ),
        (
            "' ב-מחסני השוק האורגים ח0740- בכרטיס המסתיים ב09:50  06/02/26 כרטיס דביט מתאריך",
            "debit_card",
            "מחסני השוק האורגים ח",
        ),
        (
            "שיעור ספורט12-799-0  טטיאנה סידלר:הפועלים-ביט העברה מאת",
            "bit",
            "טטיאנה סידלר",
        ),
        (
            "BIT  העברת כספים12-799-0  יאיר ביליה:הפועלים-ביט העברה מאת",
            "bit",
            "יאיר ביליה",
        ),
        (
            "עומץ10-843-002867327  שירה יהב גרנ:העברה דיגיטל העברה אל",
            "transfer",
            "שירה יהב גרנ",
        ),
        (
            "0-001-501539007 OPHER DONCHIN :העברה תוך יומי העברה מאת",
            "transfer",
            "OPHER DONCHIN",
        ),
        (
            "משכורת12-177-000006501 בן גוריו. א:אונ' בן גורי-י העברה מאת",
            "transfer",
            "אונ' בן גורי-י",
        ),
        (
            "להחזיר כסף11-045-014408329  חן הנסן: אינטרנט העברה אל.הע",
            "transfer",
            "חן הנסן",
        ),
        (
            "פרעון הלוואה",
            "loan",
            "פרעון הלוואה",
        ),
        (
            "מקס איט פיננ-י",
            "other",
            "מקס איט פיננ-י",
        ),
    ],
)
def test_extract_merchant(description_raw: str, expected_kind: str, expected_merchant: str) -> None:
    merchant_raw, txn_kind = extract_merchant(description_raw)
    assert txn_kind == expected_kind
    assert merchant_raw == expected_merchant


def test_extract_merchant_empty_falls_back_to_unknown() -> None:
    merchant_raw, txn_kind = extract_merchant("   ")
    assert txn_kind == "other"
    assert merchant_raw == "UNKNOWN"
