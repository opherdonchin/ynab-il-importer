from __future__ import annotations

import pandas as pd

import ynab_il_importer.cross_budget_pairing as cross_budget_pairing


def _source_row(
    *,
    row_id: str,
    account_name: str = "Family Leumi",
    date: str = "2025-11-01",
    payee_raw: str = "Client Payment",
    category_raw: str = "Pilates",
    outflow_ils: float = 0.0,
    inflow_ils: float = 100.0,
    txn_kind: str = "credit",
    fingerprint: str = "client payment",
    memo: str = "",
) -> dict[str, object]:
    return {
        "source": "ynab",
        "ynab_id": row_id,
        "account_name": account_name,
        "date": date,
        "payee_raw": payee_raw,
        "category_raw": category_raw,
        "outflow_ils": outflow_ils,
        "inflow_ils": inflow_ils,
        "txn_kind": txn_kind,
        "fingerprint": fingerprint,
        "memo": memo,
    }


def _target_row(
    *,
    row_id: str,
    account_name: str = "In Family",
    date: str = "2025-11-01",
    payee_raw: str = "Client Payment",
    category_raw: str = "Pilates Income",
    outflow_ils: float = 0.0,
    inflow_ils: float = 100.0,
    txn_kind: str = "credit",
    fingerprint: str = "client payment",
    memo: str = "",
) -> dict[str, object]:
    return {
        "source": "ynab",
        "ynab_id": row_id,
        "account_id": f"acc-{row_id}",
        "account_name": account_name,
        "date": date,
        "payee_raw": payee_raw,
        "category_raw": category_raw,
        "outflow_ils": outflow_ils,
        "inflow_ils": inflow_ils,
        "txn_kind": txn_kind,
        "fingerprint": fingerprint,
        "memo": memo,
        "import_id": "",
        "matched_transaction_id": "",
        "cleared": "uncleared",
        "approved": False,
    }


def test_unique_exact_match_allows_different_source_account() -> None:
    source_df = pd.DataFrame([_source_row(row_id="source-1")])
    target_df = pd.DataFrame([_target_row(row_id="target-1")])

    result = cross_budget_pairing.match_cross_budget_rows(
        source_df,
        target_df,
        target_account="In Family",
        source_category="Pilates",
    )

    assert len(result.matched_pairs_df) == 1
    assert result.unmatched_source_df.empty
    assert result.unmatched_target_df.empty
    assert result.ambiguous_matches_df.empty
    assert result.matched_pairs_df.loc[0, "ynab_account"] == "In Family"
    assert result.matched_pairs_df.loc[0, "match_type"] == "exact_date_amount"


def test_target_account_filter_ignores_other_target_accounts() -> None:
    source_df = pd.DataFrame([_source_row(row_id="source-1")])
    target_df = pd.DataFrame(
        [
            _target_row(row_id="target-good", account_name="In Family"),
            _target_row(row_id="target-other", account_name="Some Other Account"),
        ]
    )

    result = cross_budget_pairing.match_cross_budget_rows(
        source_df,
        target_df,
        target_account="In Family",
        source_category="Pilates",
    )

    assert len(result.matched_pairs_df) == 1
    assert result.matched_pairs_df.loc[0, "target_row_id"] == "target-good"


def test_transfer_partitioning_prevents_cross_kind_match() -> None:
    source_df = pd.DataFrame(
        [
            _source_row(
                row_id="source-transfer",
                payee_raw="Transfer : Pilates Leumi",
                txn_kind="transfer",
                outflow_ils=100.0,
                inflow_ils=0.0,
                fingerprint="transfer pilates leumi",
            ),
            _source_row(
                row_id="source-ordinary",
                payee_raw="Office Rent",
                txn_kind="expense",
                outflow_ils=100.0,
                inflow_ils=0.0,
                fingerprint="office rent",
            ),
        ]
    )
    target_df = pd.DataFrame(
        [
            _target_row(
                row_id="target-transfer",
                payee_raw="Transfer : Bank Leumi 225237",
                txn_kind="transfer",
                outflow_ils=100.0,
                inflow_ils=0.0,
                fingerprint="transfer bank leumi",
            ),
            _target_row(
                row_id="target-ordinary",
                payee_raw="Office Rent",
                txn_kind="expense",
                outflow_ils=100.0,
                inflow_ils=0.0,
                fingerprint="office rent",
            ),
        ]
    )

    result = cross_budget_pairing.match_cross_budget_rows(
        source_df,
        target_df,
        target_account="In Family",
        source_category="Pilates",
    )

    assert len(result.matched_pairs_df) == 2
    assert set(result.matched_pairs_df["row_kind"].tolist()) == {
        "transfer_like",
        "ordinary",
    }


def test_loan_source_row_matches_transfer_target() -> None:
    source_df = pd.DataFrame(
        [
            _source_row(
                row_id="source-loan",
                payee_raw="Loan Pilates",
                category_raw="Pilates",
                outflow_ils=1842.38,
                inflow_ils=0.0,
                txn_kind="expense",
                fingerprint="loan pilates",
                memo="פרעון הלוואה",
            )
        ]
    )
    target_df = pd.DataFrame(
        [
            _target_row(
                row_id="target-loan",
                payee_raw="Transfer : Leumi loan 64370054",
                category_raw="Leumi loan 64370054",
                outflow_ils=1842.38,
                inflow_ils=0.0,
                txn_kind="transfer",
                fingerprint="transfer leumi loan",
                memo="פרעון הלוואה",
            )
        ]
    )

    result = cross_budget_pairing.match_cross_budget_rows(
        source_df,
        target_df,
        target_account="In Family",
        source_category="Pilates",
    )

    assert len(result.matched_pairs_df) == 1
    assert result.unmatched_source_df.empty
    assert result.unmatched_target_df.empty
    assert result.matched_pairs_df.loc[0, "row_kind"] == "transfer_like"


def test_ambiguous_same_bucket_is_left_unresolved() -> None:
    source_df = pd.DataFrame([_source_row(row_id="source-1")])
    target_df = pd.DataFrame(
        [
            _target_row(row_id="target-1"),
            _target_row(row_id="target-2", payee_raw="Another Row"),
        ]
    )

    result = cross_budget_pairing.match_cross_budget_rows(
        source_df,
        target_df,
        target_account="In Family",
        source_category="Pilates",
    )

    assert result.matched_pairs_df.empty
    assert result.unmatched_source_df.empty
    assert result.unmatched_target_df.empty
    assert len(result.ambiguous_matches_df) == 1
    assert result.ambiguous_matches_df.loc[0, "reason"] == "same_date_amount_bucket_not_unique"


def test_date_window_text_tiebreak_resolves_bootstrap_candidate() -> None:
    source_df = pd.DataFrame(
        [
            _source_row(
                row_id="source-1",
                date="2025-11-02",
                payee_raw="Salary Liya",
                fingerprint="salary liya",
                inflow_ils=0.0,
                outflow_ils=100.0,
                txn_kind="expense",
            )
        ]
    )
    target_df = pd.DataFrame(
        [
            _target_row(
                row_id="target-wrong",
                date="2025-11-01",
                payee_raw="Other Payee",
                fingerprint="other payee",
                inflow_ils=0.0,
                outflow_ils=100.0,
                txn_kind="expense",
            ),
            _target_row(
                row_id="target-right",
                date="2025-11-03",
                payee_raw="Salary Liya",
                fingerprint="salary liya",
                inflow_ils=0.0,
                outflow_ils=100.0,
                txn_kind="expense",
            ),
        ]
    )

    result = cross_budget_pairing.match_cross_budget_rows(
        source_df,
        target_df,
        target_account="In Family",
        source_category="Pilates",
        date_tolerance_days=1,
    )

    assert len(result.matched_pairs_df) == 1
    assert result.matched_pairs_df.loc[0, "target_row_id"] == "target-right"
    assert result.matched_pairs_df.loc[0, "match_type"] == "date_window_text_tiebreak"


def test_manual_target_row_remains_unmatched_when_source_category_missing() -> None:
    source_df = pd.DataFrame(
        [
            _source_row(
                row_id="source-ignored",
                category_raw="Uncategorized",
                payee_raw="Adva Shtainberg",
                inflow_ils=0.0,
                outflow_ils=250.0,
                txn_kind="expense",
                fingerprint="adva shtainberg",
            )
        ]
    )
    target_df = pd.DataFrame(
        [
            _target_row(
                row_id="target-manual",
                payee_raw="Adva Shtainberg",
                inflow_ils=0.0,
                outflow_ils=250.0,
                txn_kind="expense",
                fingerprint="adva shtainberg",
            )
        ]
    )

    result = cross_budget_pairing.match_cross_budget_rows(
        source_df,
        target_df,
        target_account="In Family",
        source_category="Pilates",
    )

    assert result.matched_pairs_df.empty
    assert result.unmatched_source_df.empty
    assert len(result.unmatched_target_df) == 1
    assert result.unmatched_target_df.loc[0, "target_row_id"] == "target-manual"
