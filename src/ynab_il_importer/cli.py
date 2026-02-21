
import argparse
from pathlib import Path
from ynab_il_importer.io_bank import read_bank
from ynab_il_importer.io_ynab import read_ynab_register

try:
    import typer
except ModuleNotFoundError:  # pragma: no cover - fallback for bare Python envs
    typer = None


if typer is not None:
    app = typer.Typer(help="YNAB IL Importer CLI")

    @app.command("parse-bank")
    def parse_bank(
        in_path: Path = typer.Option(..., "--in"),
        out_path: Path = typer.Option(..., "--out"),
    ) -> None:
        df = read_bank(in_path)
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        typer.echo(f"Wrote {len(df)} rows to {out_path}")

    @app.command("parse-card")
    def parse_card(
        in_path: Path = typer.Option(..., "--in"),
        out_path: Path = typer.Option(..., "--out"),
    ) -> None:
        typer.echo(f"Would parse card file from {in_path} to {out_path}")

    @app.command("parse-ynab")
    def parse_ynab(
        in_path: Path = typer.Option(..., "--in"),
        out_path: Path = typer.Option(..., "--out"),
    ) -> None:
        df = read_ynab_register(in_path)
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"Wrote {len(df)} rows to {out_path}")

    @app.command("match-pairs")
    def match_pairs(
        bank_path: Path = typer.Option(..., "--bank"),
        card_path: Path = typer.Option(..., "--card"),
        ynab_path: Path = typer.Option(..., "--ynab"),
        out_path: Path = typer.Option(..., "--out"),
    ) -> None:
        typer.echo(
            "Would match pairs using "
            f"bank={bank_path}, card={card_path}, ynab={ynab_path}, output={out_path}"
        )

    @app.command("build-groups")
    def build_groups(
        pairs_path: Path = typer.Option(..., "--pairs"),
        out_path: Path = typer.Option(..., "--out"),
    ) -> None:
        typer.echo(f"Would build fingerprint groups from {pairs_path} to {out_path}")
else:
    app = None


def _fallback_main() -> None:
    parser = argparse.ArgumentParser(prog="ynab-il", description="YNAB IL Importer CLI")
    subparsers = parser.add_subparsers(dest="command")

    parse_bank_parser = subparsers.add_parser("parse-bank")
    parse_bank_parser.add_argument("--in", dest="in_path", required=True)
    parse_bank_parser.add_argument("--out", dest="out_path", required=True)

    parse_card_parser = subparsers.add_parser("parse-card")
    parse_card_parser.add_argument("--in", dest="in_path", required=True)
    parse_card_parser.add_argument("--out", dest="out_path", required=True)

    parse_ynab_parser = subparsers.add_parser("parse-ynab")
    parse_ynab_parser.add_argument("--in", dest="in_path", required=True)
    parse_ynab_parser.add_argument("--out", dest="out_path", required=True)

    match_pairs_parser = subparsers.add_parser("match-pairs")
    match_pairs_parser.add_argument("--bank", required=True)
    match_pairs_parser.add_argument("--card", required=True)
    match_pairs_parser.add_argument("--ynab", required=True)
    match_pairs_parser.add_argument("--out", required=True)

    build_groups_parser = subparsers.add_parser("build-groups")
    build_groups_parser.add_argument("--pairs", required=True)
    build_groups_parser.add_argument("--out", required=True)

    args = parser.parse_args()
    if args.command == "parse-bank":
        df = read_bank(args.in_path)
        Path(args.out_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(args.out_path, index=False, encoding="utf-8-sig")
        print(f"Wrote {len(df)} rows to {args.out_path}")
    elif args.command == "parse-card":
        print(f"Would parse card file from {args.in_path} to {args.out_path}")
    elif args.command == "parse-ynab":
        df = read_ynab_register(args.in_path)
        Path(args.out_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(args.out_path, index=False, encoding="utf-8-sig")
        print(f"Wrote {len(df)} rows to {args.out_path}")
    elif args.command == "match-pairs":
        print(
            "Would match pairs using "
            f"bank={args.bank}, card={args.card}, ynab={args.ynab}, output={args.out}"
        )
    elif args.command == "build-groups":
        print(f"Would build fingerprint groups from {args.pairs} to {args.out}")


def main() -> None:
    if typer is not None:
        app()
    else:
        _fallback_main()


if __name__ == "__main__":
    main()
