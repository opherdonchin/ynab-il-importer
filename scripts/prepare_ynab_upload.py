# ruff: noqa: E402

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.export as export
import ynab_il_importer.upload_prep as upload_prep
import ynab_il_importer.workflow_profiles as workflow_profiles
import ynab_il_importer.ynab_api as ynab_api
from ynab_il_importer.safe_types import normalize_flag_series
import polars as pl


def _default_csv_out(input_path: Path) -> Path:
    stem = input_path.with_suffix("") if input_path.suffix else input_path
    return Path(f"{stem}_upload.csv")


def _default_json_out(csv_out: Path) -> Path:
    return csv_out.with_suffix(".json")


def _print_section(title: str, rows: list[tuple[str, object]]) -> None:
    print(f"\n{title}")
    for label, value in rows:
        print(f"  {label:<28} {value}")


def _print_messages(title: str, messages: list[str]) -> None:
    if not messages:
        return
    print(f"\n{title}")
    for message in messages:
        print(f"  - {message}")


def _parse_bool_arg(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"true", "1", "yes"}:
        return True
    if normalized in {"false", "0", "no"}:
        return False
    raise argparse.ArgumentTypeError(
        f"invalid boolean value: {value!r} (expected true/false)"
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare reviewed transactions for YNAB upload"
    )
    parser.add_argument(
        "--in", dest="input_path", required=True, help="Reviewed review-artifact path."
    )
    parser.add_argument(
        "--out",
        dest="out_path",
        default="",
        help="Prepared upload CSV path. Defaults to <input>_upload.csv.",
    )
    parser.add_argument(
        "--json-out",
        dest="json_out_path",
        default="",
        help="Payload JSON path. Defaults to <out>.json.",
    )
    parser.add_argument(
        "--cleared",
        choices=["cleared", "uncleared"],
        default="cleared",
        help="Cleared status to send to YNAB.",
    )
    parser.add_argument(
        "--approved",
        type=_parse_bool_arg,
        default=False,
        metavar="BOOL",
        help="Approved flag sent to YNAB (default: false). Pass true/false.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Post the prepared transactions to YNAB after writing the dry-run artifacts.",
    )
    parser.add_argument(
        "--ready-only",
        action="store_true",
        help="Prepare only rows that are currently ready for upload.",
    )
    parser.add_argument(
        "--reviewed-only",
        action="store_true",
        help="Prepare only rows marked reviewed in the reviewed CSV.",
    )
    parser.add_argument(
        "--skip-missing-accounts",
        action="store_true",
        help="Skip rows whose account_name does not map to a live YNAB account.",
    )
    parser.add_argument("--profile", default="", help="Workflow profile (for budget defaults).")
    parser.add_argument("--budget-id", dest="budget_id", default="", help="Override YNAB budget/plan id.")
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    input_path = Path(args.input_path)
    out_path = Path(args.out_path) if args.out_path else _default_csv_out(input_path)
    json_out_path = (
        Path(args.json_out_path) if args.json_out_path else _default_json_out(out_path)
    )
    profile = workflow_profiles.resolve_profile(args.profile or None)
    plan_id = workflow_profiles.resolve_budget_id(
        profile=profile.name,
        budget_id=args.budget_id,
    )

    reviewed = upload_prep.load_upload_working_frame(input_path)
    accounts = ynab_api.fetch_accounts(plan_id=plan_id or None)
    if args.reviewed_only:
        reviewed = reviewed.filter(
            pl.Series(
                normalize_flag_series(reviewed["reviewed"].to_pandas())
                .astype(bool)
                .tolist()
            )
        )
    if args.ready_only:
        reviewed = reviewed.filter(
            pl.Series("ready_mask", upload_prep.ready_mask(reviewed).astype(bool).tolist())
        )
    if args.skip_missing_accounts:
        account_mask = upload_prep.uploadable_account_mask(reviewed, accounts)
        skipped = int((~account_mask).sum())
        if skipped:
            print(f"Skipping {skipped} rows with missing/unmapped account_name values.")
        reviewed = reviewed.filter(
            pl.Series("account_mask", account_mask.astype(bool).tolist())
        )
    if reviewed.is_empty():
        raise ValueError("No rows remain after applying the selected upload filters.")
    existing_transactions = ynab_api.fetch_transactions(plan_id=plan_id or None)
    category_groups = ynab_api.fetch_categories(plan_id=plan_id or None)
    categories = ynab_api.categories_to_dataframe(category_groups)
    if categories.empty:
        categories = ynab_api.categories_from_transactions_to_dataframe(
            existing_transactions
        )
        if not categories.empty:
            print(
                "YNAB categories API returned no rows; using category ids inferred from existing transactions."
            )

    prepared = upload_prep.prepare_upload_transactions(
        reviewed,
        accounts=accounts,
        categories_df=categories,
        cleared=args.cleared,
        approved=args.approved,
    )
    if prepared.empty:
        raise ValueError(
            "No create_target rows remain after applying the selected upload filters."
        )
    preflight = upload_prep.upload_preflight(prepared, existing_transactions)
    if preflight["unsupported_transaction_unit_ids"]:
        raise ValueError(
            "Unsupported upload transaction units: "
            + ", ".join(preflight["unsupported_transaction_unit_ids"])
        )
    payload = upload_prep.upload_payload_records(prepared)

    export.write_dataframe(prepared, out_path)
    json_out_path.parent.mkdir(parents=True, exist_ok=True)
    json_out_path.write_text(
        json.dumps({"transactions": payload}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    _print_section(
        "Artifacts",
        [
            ("prepared CSV", export.display_path(out_path)),
            ("payload JSON", export.display_path(json_out_path)),
            ("prepared rows", len(prepared)),
        ],
    )
    _print_section(
        "Upload preflight",
        [
            ("prepared rows", preflight["prepared_count"]),
            ("transfer rows", preflight["transfer_count"]),
            ("split rows", preflight["split_count"]),
            ("duplicate payload keys", len(preflight["payload_duplicate_import_keys"])),
            ("existing import_id hits", len(preflight["existing_import_id_hits"])),
            ("possible manual matches", len(preflight["potential_match_import_ids"])),
            ("transfer payload issues", len(preflight["transfer_payload_issue_ids"])),
        ],
    )
    preflight_notes: list[str] = []
    if preflight["existing_import_id_hits"]:
        preflight_notes.append(
            "Some import_ids already exist in YNAB. A rerun should report those rows as duplicates."
        )
    if preflight["potential_match_import_ids"]:
        preflight_notes.append(
            "Some rows may match existing user-entered transactions instead of creating brand-new imports."
        )
    _print_messages("Notes", preflight_notes)
    if preflight["payload_duplicate_import_keys"]:
        raise ValueError(
            "Payload contains duplicate (account_id, import_id) keys: "
            + ", ".join(
                [
                    f"{account_id}::{import_id}"
                    for account_id, import_id in preflight[
                        "payload_duplicate_import_keys"
                    ]
                ]
            )
        )

    if args.execute:
        response = ynab_api.create_transactions(payload, plan_id=plan_id or None)
        summary = upload_prep.summarize_upload_response(response)
        outcome = upload_prep.classify_upload_result(
            summary, prepared_count=len(prepared)
        )
        _print_section(
            "Upload result",
            [
                ("newly saved", outcome["saved"]),
                ("duplicate import_ids", outcome["duplicate_import_ids"]),
                ("matched existing", outcome["matched_existing"]),
                ("transfer rows returned", outcome["transfer_saved"]),
                ("split rows returned", outcome["split_saved"]),
                ("status", outcome["status"]),
            ],
        )

        upload_notes: list[str] = []
        if outcome["matched_existing"]:
            upload_notes.append(
                "Some rows matched existing user-entered transactions rather than creating new imports."
            )
        if outcome["idempotent_rerun"]:
            upload_notes.append("All payload rows were already present in YNAB.")
        _print_messages("Upload notes", upload_notes)

        if outcome["verification_needed"]:
            verification = upload_prep.verify_upload_response(prepared, response)
            _print_section(
                "Upload verification",
                [
                    ("checked", verification["checked"]),
                    (
                        "missing saved txns",
                        len(verification["missing_saved_transactions"]),
                    ),
                    ("amount mismatches", len(verification["amount_mismatches"])),
                    ("date mismatches", len(verification["date_mismatches"])),
                    ("account mismatches", len(verification["account_mismatches"])),
                    ("transfer mismatches", len(verification["transfer_mismatches"])),
                    ("category mismatches", len(verification["category_mismatches"])),
                    ("split mismatches", len(verification["split_mismatches"])),
                ],
            )
            upload_warnings: list[str] = []
            if verification["transfer_mismatches"]:
                upload_warnings.append(
                    "Some transfer rows did not come back as the expected transfer."
                )
            if verification["split_mismatches"]:
                upload_warnings.append(
                    "Some split rows did not come back with the expected split child structure."
                )
            if verification["missing_saved_transactions"]:
                upload_warnings.append(
                    "Some saved transaction_ids were not present in the response transaction list."
                )
            _print_messages("Warnings", upload_warnings)
        else:
            _print_section(
                "Upload verification",
                [
                    ("status", "skipped"),
                    ("reason", "no newly saved transactions returned by YNAB"),
                ],
            )


if __name__ == "__main__":
    main()

