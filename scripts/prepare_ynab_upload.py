import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.export as export
import ynab_il_importer.review_app.io as review_io
import ynab_il_importer.upload_prep as upload_prep
import ynab_il_importer.ynab_api as ynab_api


def _default_csv_out(input_path: Path) -> Path:
    suffix = input_path.suffix or ".csv"
    stem = input_path.with_suffix("") if input_path.suffix else input_path
    return Path(f"{stem}_upload{suffix}")


def _default_json_out(csv_out: Path) -> Path:
    return csv_out.with_suffix(".json")


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare reviewed transactions for YNAB upload")
    parser.add_argument("--in", dest="input_path", required=True, help="Reviewed transactions CSV.")
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
    args = parser.parse_args()

    input_path = Path(args.input_path)
    out_path = Path(args.out_path) if args.out_path else _default_csv_out(input_path)
    json_out_path = Path(args.json_out_path) if args.json_out_path else _default_json_out(out_path)

    reviewed = review_io.load_proposed_transactions(input_path)
    accounts = ynab_api.fetch_accounts()
    if args.reviewed_only:
        reviewed = reviewed[reviewed["reviewed"].astype(bool)].copy()
    if args.ready_only:
        reviewed = reviewed[upload_prep.ready_mask(reviewed)].copy()
    if args.skip_missing_accounts:
        account_mask = upload_prep.uploadable_account_mask(reviewed, accounts)
        skipped = int((~account_mask).sum())
        if skipped:
            print(f"Skipping {skipped} rows with missing/unmapped account_name values.")
        reviewed = reviewed[account_mask].copy()
    if reviewed.empty:
        raise ValueError("No rows remain after applying the selected upload filters.")
    categories = ynab_api.categories_to_dataframe(ynab_api.fetch_categories())
    existing_transactions = ynab_api.fetch_transactions()

    prepared = upload_prep.prepare_upload_transactions(
        reviewed,
        accounts=accounts,
        categories_df=categories,
        cleared=args.cleared,
        approved=True,
    )
    payload = upload_prep.upload_payload_records(prepared)
    preflight = upload_prep.upload_preflight(prepared, existing_transactions)

    export.write_dataframe(prepared, out_path)
    json_out_path.parent.mkdir(parents=True, exist_ok=True)
    json_out_path.write_text(
        json.dumps({"transactions": payload}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"Wrote {out_path} ({len(prepared)} rows)")
    print(f"Wrote {json_out_path}")
    print(
        "Upload preflight: "
        f"prepared={preflight['prepared_count']}, "
        f"transfers={preflight['transfer_count']}, "
        f"payload_duplicate_import_keys={len(preflight['payload_duplicate_import_keys'])}, "
        f"existing_import_id_hits={len(preflight['existing_import_id_hits'])}, "
        f"potential_match_candidates={len(preflight['potential_match_import_ids'])}, "
        f"transfer_payload_issues={len(preflight['transfer_payload_issue_ids'])}"
    )
    if preflight["existing_import_id_hits"]:
        print(
            "Preflight note: some import_ids already exist in YNAB; "
            "a rerun should treat those rows as duplicates."
        )
    if preflight["potential_match_import_ids"]:
        print(
            "Preflight note: some rows may match existing user-entered transactions "
            "instead of creating brand-new imports."
        )
    if preflight["payload_duplicate_import_keys"]:
        raise ValueError(
            "Payload contains duplicate (account_id, import_id) keys: "
            + ", ".join(
                [f"{account_id}::{import_id}" for account_id, import_id in preflight["payload_duplicate_import_keys"]]
            )
        )

    if args.execute:
        response = ynab_api.create_transactions(payload)
        summary = upload_prep.summarize_upload_response(response)
        verification = upload_prep.verify_upload_response(prepared, response)
        print(
            "YNAB upload complete: "
            f"saved={summary['saved']}, "
            f"duplicate_import_ids={summary['duplicate_import_ids']}, "
            f"matched_existing={summary['matched_existing']}, "
            f"transfer_saved={summary['transfer_saved']}"
        )
        if summary["matched_existing"]:
            print(
                "Upload note: some rows matched existing user-entered transactions "
                "rather than creating new imported rows."
            )
        if summary["transfer_saved"] != preflight["transfer_count"]:
            print(
                "Upload warning: the number of saved transfer transactions does not "
                "match the number of transfer rows in the payload."
            )
        print(
            "Upload verification: "
            f"checked={verification['checked']}, "
            f"missing_saved_transactions={len(verification['missing_saved_transactions'])}, "
            f"amount_mismatches={len(verification['amount_mismatches'])}, "
            f"date_mismatches={len(verification['date_mismatches'])}, "
            f"account_mismatches={len(verification['account_mismatches'])}, "
            f"transfer_mismatches={len(verification['transfer_mismatches'])}, "
            f"category_mismatches={len(verification['category_mismatches'])}"
        )
        if verification["transfer_mismatches"]:
            print(
                "Upload warning: some transfer rows did not come back as the expected transfer."
            )


if __name__ == "__main__":
    main()
