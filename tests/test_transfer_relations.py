from __future__ import annotations

import polars as pl

import ynab_il_importer.review_app.transfer_relations as transfer_relations


def _transfer_row(
    *,
    account_name: str,
    peer_account: str,
    date: str = "2026-04-10",
    outflow_ils: float = 100.0,
    inflow_ils: float = 0.0,
    source_present: bool = True,
    target_present: bool = False,
    decision_action: str = "No decision",
    reviewed: bool = False,
) -> dict[str, object]:
    return {
        "account_name": account_name,
        "source_account": account_name,
        "target_account": account_name,
        "date": date,
        "source_date": date,
        "target_date": "",
        "outflow_ils": outflow_ils,
        "inflow_ils": inflow_ils,
        "source_present": source_present,
        "target_present": target_present,
        "source_row_id": f"src-{account_name}-{date}-{outflow_ils}-{inflow_ils}",
        "target_row_id": f"tgt-{account_name}-{date}-{outflow_ils}-{inflow_ils}" if target_present else "",
        "source_payee_current": "",
        "source_payee_selected": f"Transfer : {peer_account}",
        "target_payee_current": "",
        "target_payee_selected": f"Transfer : {peer_account}",
        "fingerprint": "transfer bank leumi",
        "memo": "",
        "decision_action": decision_action,
        "reviewed": reviewed,
        "target_account_on_budget": True,
        "source_account_on_budget": True,
        "target_transfer_account_on_budget": True,
        "source_transfer_account_on_budget": True,
        "target_category_selected": "None",
    }


def test_build_transfer_relation_frame_pairs_two_internal_transfer_rows() -> None:
    df = pl.DataFrame(
        [
            _transfer_row(account_name="Bank Leumi", peer_account="Cash", outflow_ils=100.0),
            _transfer_row(account_name="Cash", peer_account="Bank Leumi", inflow_ils=100.0, outflow_ils=0.0),
        ]
    )

    relations = transfer_relations.build_transfer_relation_frame(df)

    assert relations.height == 1
    relation = relations.row(0, named=True)
    assert relation["relation_kind"] == "internal_budget"
    assert relation["relation_status"] == "fully_visible_in_review"
    assert relation["peer_review_row_present"] is True
    assert relation["ambiguous_relation"] is False
    assert relation["row_positions"] == [0, 1]


def test_build_transfer_relation_frame_marks_missing_peer_source() -> None:
    df = pl.DataFrame(
        [
            _transfer_row(account_name="Bank Leumi", peer_account="Cash", outflow_ils=100.0),
        ]
    )

    relations = transfer_relations.build_transfer_relation_frame(df)

    assert relations.height == 1
    relation = relations.row(0, named=True)
    assert relation["relation_status"] == "peer_source_missing_this_run"
    assert relation["peer_review_row_present"] is False
    assert relation["account_a"] == "Bank Leumi"
    assert relation["account_b"] == "Cash"


def test_apply_transfer_relation_propagates_decision_to_member_rows() -> None:
    df = pl.DataFrame(
        [
            _transfer_row(account_name="Bank Leumi", peer_account="Cash", outflow_ils=100.0),
            _transfer_row(account_name="Cash", peer_account="Bank Leumi", inflow_ils=100.0, outflow_ils=0.0),
        ]
    )
    relation = transfer_relations.build_transfer_relation_frame(df).row(0, named=True)

    updated, affected_indices, errors = transfer_relations.apply_transfer_relation(
        df,
        relation,
        decision_action="create_target",
    )

    assert errors == []
    assert affected_indices == [0, 1]
    assert updated.get_column("decision_action").to_list() == ["create_target", "create_target"]
