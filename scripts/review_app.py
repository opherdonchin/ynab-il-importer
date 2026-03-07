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

    if getattr(args, "app_help", False):
        parser.print_help()
        return

    app_path = ROOT / "src" / "ynab_il_importer" / "review_app" / "app.py"
    cmd = [sys.executable, "-m", "streamlit", "run", str(app_path), "--"]
    cmd.extend(sys.argv[1:])
    subprocess.run(cmd, check=True)


if __name__ == "__main__":
    main()
