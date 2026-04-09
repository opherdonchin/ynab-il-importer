import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.card_identity as card_identity
import ynab_il_importer.io_leumi_card_html as leumi_card_html


HTML_FIXTURE = """\
<html lang="he">
  <body>
    <div class="credit-card-activity-tpl">
      <div xltopright="">
        <span>פרוט עסקאות לכרטיס</span>
        <span>לאומי ויזה</span>
        <span>0602</span>
        <span>לתקופה:</span>
        <span>אפריל 2026</span>
      </div>

      <div role="table" class="ts-table my-3 w-100" aria-label="עסקאות אחרונות שטרם נקלטו">
        <div role="row" class="ts-table-row ts-table-header gray-bg">
          <div role="columnheader" xlheader="1"><span>תאריך העסקה</span></div>
          <div role="columnheader" xlheader="2"><span>שעה</span></div>
          <div role="columnheader" xlheader="3"><span>שם בית העסק</span></div>
          <div role="columnheader" xlheader="4"><span>סוג העסקה</span></div>
          <div role="columnheader" xlheader="5"><span>פרטים</span></div>
          <div role="columnheader" xlheader="6"><span>סכום העסקה</span></div>
        </div>
        <section role="presentation" xlrow="" class="cc-table-entry expand-item ng-star-inserted">
          <button role="row" class="ts-table-row hovered is-icon">
            <div role="cell" xlcell="1" data-header="תאריך העסקה"><span class="ts-num show-exporttool">19.03.26</span></div>
            <div role="cell" xlcell="2" data-header="שעה"><span class="ts-num show-exporttool">18:29</span></div>
            <div role="cell" xlcell="3" data-header="שם בית העסק"><span>FACEBK *6NMHMGR8C2</span></div>
            <div role="cell" xlcell="4" data-header="סוג העסקה"><span>עסקה רגילה</span></div>
            <div role="cell" xlcell="5" data-header="פרטים"><span></span></div>
            <div role="cell" xlcell="6" data-header="סכום העסקה"><span>100.00</span></div>
          </button>
        </section>
      </div>

      <div role="table" class="ts-table my-3 w-100" aria-label="עסקאות בש&quot;ח במועד החיוב">
        <div role="row" class="ts-table-row ts-table-header gray-bg">
          <div role="columnheader" xlheader="1"><span>תאריך העסקה</span></div>
          <div role="columnheader" xlheader="2"><span>שם בית העסק</span></div>
          <div role="columnheader" xlheader="3"><span>סכום העסקה</span></div>
          <div role="columnheader" xlheader="4"><span>סוג העסקה</span></div>
          <div role="columnheader" xlheader="5"><span>פרטים</span></div>
          <div role="columnheader" xlheader="6"><span>סכום חיוב</span></div>
        </div>
        <section role="presentation" xlrow="" class="cc-table-entry expand-item ng-star-inserted">
          <button role="row" class="ts-table-row hovered is-icon">
            <div role="cell" xlcell="1" data-header="תאריך העסקה"><span class="ts-num show-exporttool">17/03/26</span></div>
            <div role="cell" xlcell="2" data-header="שם בית העסק"><span>FACEBK *2SYRAHM8C2</span></div>
            <div role="cell" xlcell="3" data-header="סכום העסקה"><span>-25.00</span></div>
            <div role="cell" xlcell="4" data-header="סוג העסקה"><span>סייקל חו&quot;ל</span></div>
            <div role="cell" xlcell="5" data-header="פרטים"><span>החזר</span></div>
            <div role="cell" xlcell="6" data-header="סכום חיוב"><span>-25.00</span></div>
          </button>
        </section>
      </div>
    </div>
  </body>
</html>
"""


def test_is_proper_format_detects_leumi_html(tmp_path: Path) -> None:
    html_path = tmp_path / "pilates card sample.html"
    html_path.write_text(HTML_FIXTURE, encoding="utf-8")

    assert leumi_card_html.is_proper_format(html_path)
    assert not leumi_card_html.is_proper_format(tmp_path / "not-a-card.html")


def test_read_raw_parses_pending_and_billed_rows(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    html_path = tmp_path / "pilates card sample.html"
    html_path.write_text(HTML_FIXTURE, encoding="utf-8")

    captured: dict[str, object] = {}

    def _fake_apply_account_name_map(
        df: pd.DataFrame,
        source: str,
        account_map_path: str | Path | None = None,
    ) -> pd.DataFrame:
        captured["account_map_source"] = source
        captured["account_map_path"] = account_map_path
        return df

    def _fake_apply_fingerprints(
        df: pd.DataFrame,
        map_rules: list | None = None,
        log_path: str | Path = Path("outputs/fingerprint_log.csv"),
        use_fingerprint_map: bool = True,
        fingerprint_map_path: str | Path = Path("mappings/fingerprint_map.csv"),
    ) -> pd.DataFrame:
        _ = map_rules
        _ = log_path
        _ = fingerprint_map_path
        captured["use_fingerprint_map"] = use_fingerprint_map
        out = df.copy()
        text = out["description_clean"].astype("string").fillna("")
        out["description_clean_norm"] = text
        out["fingerprint"] = text.str.lower()
        return out

    monkeypatch.setattr(
        "ynab_il_importer.io_leumi_card_html.account_map.apply_account_name_map",
        _fake_apply_account_name_map,
    )
    monkeypatch.setattr(
        "ynab_il_importer.io_leumi_card_html.fingerprint.apply_fingerprints",
        _fake_apply_fingerprints,
    )

    account_map_path = tmp_path / "account_name_map.csv"
    actual = leumi_card_html.read_raw(
        html_path,
        use_fingerprint_map=False,
        account_map_path=account_map_path,
    )

    assert captured["account_map_source"] == "card"
    assert captured["account_map_path"] == account_map_path
    assert captured["use_fingerprint_map"] is False

    assert len(actual) == 2
    assert actual["source"].tolist() == ["card", "card"]
    assert actual["source_account"].tolist() == ["x0602", "x0602"]
    assert actual["account_name"].tolist() == ["x0602", "x0602"]
    assert actual["card_suffix"].tolist() == ["0602", "0602"]
    assert actual["max_sheet"].tolist() == [
        "עסקאות אחרונות שטרם נקלטו",
        "עסקאות בש\"ח במועד החיוב",
    ]
    assert actual["max_report_owner"].tolist() == ["לאומי ויזה", "לאומי ויזה"]
    assert actual["max_report_scope"].tolist() == ["0602", "0602"]
    assert actual["max_report_period"].tolist() == ["אפריל 2026", "אפריל 2026"]
    assert actual["max_txn_type"].tolist() == ["עסקה רגילה", "סייקל חו\"ל"]
    assert actual["max_is_pending"].tolist() == [True, False]
    assert actual["max_time"].tolist() == ["18:29", ""]
    assert actual["max_details"].tolist() == ["", "החזר"]
    assert actual["txn_kind"].tolist() == ["expense", "credit"]
    assert actual["merchant_raw"].tolist() == [
        "FACEBK *6NMHMGR8C2",
        "FACEBK *2SYRAHM8C2",
    ]
    assert actual["description_raw"].tolist() == [
        "FACEBK *6NMHMGR8C2",
        "FACEBK *2SYRAHM8C2 | החזר",
    ]
    assert actual["description_clean"].tolist() == [
        "FACEBK *6NMHMGR8C2",
        "FACEBK *2SYRAHM8C2",
    ]
    assert actual["description_clean_norm"].tolist() == [
        "FACEBK *6NMHMGR8C2",
        "FACEBK *2SYRAHM8C2",
    ]
    assert actual["fingerprint"].tolist() == [
        "facebk *6nmhmgr8c2",
        "facebk *2syrahm8c2",
    ]
    assert actual["outflow_ils"].tolist() == [100.0, 0.0]
    assert actual["inflow_ils"].tolist() == [0.0, 25.0]
    assert actual["currency"].tolist() == ["ILS", "ILS"]
    assert actual["secondary_date"].tolist() == [pd.Timestamp("2026-04-01").date()] * 2
    assert actual["card_txn_id"].map(card_identity.is_card_txn_id).all()
