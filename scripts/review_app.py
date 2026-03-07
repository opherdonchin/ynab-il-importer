import sys
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.review_app.app as review_app


def main() -> None:
    parser = review_app._build_arg_parser()
    try:
        args = parser.parse_args()
    except SystemExit:
        return

    app_path = ROOT / "src" / "ynab_il_importer" / "review_app" / "app.py"
    input_path = Path(args.input_path)
    output_path = (
        Path(args.output_path)
        if any(arg == "--out" or arg.startswith("--out=") for arg in sys.argv[1:])
        else review_app._default_reviewed_path(input_path)
    )
    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(app_path),
        "--",
        "--in",
        str(input_path),
        "--out",
        str(output_path),
        "--categories",
        str(args.categories_path),
    ]
    if args.resume:
        cmd.extend(["--resume", str(args.resume)])
    subprocess.run(cmd, check=True)


if __name__ == "__main__":
    main()
