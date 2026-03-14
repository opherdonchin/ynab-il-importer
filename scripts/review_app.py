import argparse
import os
import signal
import socket
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.review_app.app as review_app


SESSION_ROOT = ROOT / "outputs" / "review_app_sessions"
POLL_INTERVAL_SECONDS = 0.5


def _default_output_path(input_path: Path) -> Path:
    return review_app._default_reviewed_path(input_path)


def _build_parser() -> argparse.ArgumentParser:
    parser = review_app._build_arg_parser()
    parser.description = "Launch the YNAB review app"
    parser.add_argument(
        "--foreground",
        action="store_true",
        help="Keep the wrapper attached instead of returning immediately.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=0,
        help="Preferred Streamlit port. Defaults to the first free port from 8501 upward.",
    )
    parser.add_argument("--watch-control-dir", default="", help=argparse.SUPPRESS)
    parser.add_argument("--watch-pid", type=int, default=0, help=argparse.SUPPRESS)
    return parser


def _quit_request_path(control_dir: Path) -> Path:
    return control_dir / review_app.QUIT_REQUEST_FILENAME


def _pid_exists(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _wait_for_exit(pid: int, timeout_seconds: float) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if not _pid_exists(pid):
            return True
        time.sleep(0.2)
    return not _pid_exists(pid)


def _terminate_pid(pid: int) -> None:
    if not _pid_exists(pid):
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError:
        return
    if _wait_for_exit(pid, timeout_seconds=5.0):
        return
    if sys.platform.startswith("win"):
        subprocess.run(
            ["taskkill", "/PID", str(pid), "/T", "/F"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    else:
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass


def _watch_quit_request(control_dir: Path, pid: int) -> int:
    quit_path = _quit_request_path(control_dir)
    while True:
        if quit_path.exists():
            _terminate_pid(pid)
            return 0
        if not _pid_exists(pid):
            return 0
        time.sleep(POLL_INTERVAL_SECONDS)


def _pick_port(requested_port: int) -> int:
    if requested_port:
        if not _port_available(requested_port):
            raise ValueError(f"Requested port is not available: {requested_port}")
        return requested_port

    for port in range(8501, 8521):
        if _port_available(port):
            return port

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _port_available(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("127.0.0.1", port))
        except OSError:
            return False
    return True


def _make_session_dir() -> Path:
    SESSION_ROOT.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    session_dir = SESSION_ROOT / stamp
    session_dir.mkdir(parents=True, exist_ok=False)
    return session_dir


def _build_streamlit_command(
    *,
    app_path: Path,
    input_path: Path,
    output_path: Path,
    categories_path: Path,
    resume: str | None,
    control_dir: Path,
    port: int,
) -> list[str]:
    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(app_path),
        "--server.port",
        str(port),
        "--",
        "--in",
        str(input_path),
        "--out",
        str(output_path),
        "--categories",
        str(categories_path),
        "--control-dir",
        str(control_dir),
    ]
    if resume:
        cmd.extend(["--resume", str(resume)])
    return cmd


def _launch_background_watcher(control_dir: Path, pid: int, log_path: Path) -> None:
    watcher_cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--watch-control-dir",
        str(control_dir),
        "--watch-pid",
        str(pid),
    ]
    with log_path.open("a", encoding="utf-8") as log_file:
        subprocess.Popen(
            watcher_cmd,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
            close_fds=True,
        )


def _launch_review_app(args: argparse.Namespace) -> int:
    app_path = ROOT / "src" / "ynab_il_importer" / "review_app" / "app.py"
    input_path = Path(args.input_path)
    output_path = (
        Path(args.output_path)
        if any(arg == "--out" or arg.startswith("--out=") for arg in sys.argv[1:])
        else _default_output_path(input_path)
    )
    session_dir = _make_session_dir()
    control_dir = session_dir / "control"
    control_dir.mkdir(parents=True, exist_ok=True)
    log_path = session_dir / "streamlit.log"
    port = _pick_port(int(args.port or 0))
    command = _build_streamlit_command(
        app_path=app_path,
        input_path=input_path,
        output_path=output_path,
        categories_path=Path(args.categories_path),
        resume=getattr(args, "resume", None),
        control_dir=control_dir,
        port=port,
    )

    popen_kwargs: dict[str, object] = {}
    if args.foreground:
        process = subprocess.Popen(command)
        return _wait_foreground(process, control_dir)

    with log_path.open("a", encoding="utf-8") as log_file:
        process = subprocess.Popen(
            command,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
            close_fds=True,
        )

    time.sleep(1.0)
    if process.poll() is not None:
        raise RuntimeError(
            f"Review app exited immediately. Check log: {log_path}"
        )

    _launch_background_watcher(control_dir, process.pid, log_path)
    print(f"Review app started in background at http://localhost:{port}")
    print(f"Reviewed output: {output_path}")
    print(f"Session dir: {session_dir}")
    print(f"Log: {log_path}")
    return 0


def _wait_foreground(process: subprocess.Popen[object], control_dir: Path) -> int:
    quit_path = _quit_request_path(control_dir)
    while True:
        if quit_path.exists():
            _terminate_pid(process.pid)
            return 0
        code = process.poll()
        if code is not None:
            return int(code)
        time.sleep(POLL_INTERVAL_SECONDS)


def main() -> None:
    parser = _build_parser()
    try:
        args = parser.parse_args()
    except SystemExit:
        return

    if args.watch_control_dir:
        raise SystemExit(_watch_quit_request(Path(args.watch_control_dir), int(args.watch_pid)))

    raise SystemExit(_launch_review_app(args))


if __name__ == "__main__":
    main()
