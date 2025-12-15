import argparse
import paramiko
import shlex
import sys
from pathlib import Path
from ping_workers import load_env_defaults, normalize, load_workers, WORKER_FILE, ENV_FILE
from rich.console import Console
from rich.theme import Theme
from contextlib import contextmanager

theme = Theme({"info": "cyan", "success": "green", "warning": "yellow", "error": "bold red"})
console = Console(theme=theme)

@contextmanager
def status(message: str, spinner: str = "dots"):
    with console.status(message, spinner=spinner):
        yield

def info(msg: str):
    console.print(f"[info]{msg}[/info]")

def success(msg: str):
    console.print(f"[success]{msg}[/success]")

def warning(msg: str):
    console.print(f"[warning]{msg}[/warning]")

def error(msg: str):
    console.print(f"[error]{msg}[/error]")

DEFAULT_HEAD_IP = "136.244.224.234"  # This is the IP of the machine that will be the head of the ray cluster
GRID_HEAD_IP = "136.244.224.30"  # This is the IP of the grid computer. It is not used as a worker to not overload to jump-host

def wrap_with_conda_env(command: str, conda_env: str | None):
    """
    Wrap a command so it runs inside `conda activate <env>` when `conda_env` is provided.

    Note: Non-interactive SSH sessions often don't have conda initialized, so we
    explicitly source a common `conda.sh` location.
    """
    env = normalize(conda_env)
    if not env:
        return command

    qenv = shlex.quote(env)
    conda_sh = '"$HOME/miniconda3/etc/profile.d/conda.sh"'
    return (
        f'[ -f {conda_sh} ] && source {conda_sh} || {{ echo "conda.sh not found at $HOME/miniconda3" 1>&2; exit 127; }}; '
        f"conda activate {qenv}; "
        f"{command}"
    )

def run_remote(host, username, password, command, timeout=10):
    """Run a specific command in a given host machine over SSH returning the outputs and the exit code"""
    remote_cmd = f"bash -lc {shlex.quote(command)}"  # The command to run
    
    # Establish the SSH connection
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(host, username=username, password=password, timeout=timeout, auth_timeout=timeout, banner_timeout=timeout)
        _, stdout, stderr = ssh.exec_command(remote_cmd, get_pty=False)  # Run the command
        out = stdout.read().decode(errors="replace")
        err = stderr.read().decode(errors="replace")
        exit_code = stdout.channel.recv_exit_status()
        return exit_code, out, err
    finally:
        try:
            ssh.close()  # Close the connection
        except Exception:
            pass

def require_ray(host, username, password, *, conda_env=None, timeout=10):
    """Check if the given host machine has the ray command accessible"""
    cmd = wrap_with_conda_env("command -v ray", conda_env)
    exit_code, _out, _err = run_remote(host, username, password, cmd, timeout=timeout)
    if exit_code != 0:
        raise RuntimeError(f"ray not available on {host}")

def head_state(host, username, password, timeout=10):
    """Determine the state of the head node including whether it is running ray as a worker or host, or not running ray at all"""
    _, out, _err = run_remote(host, username, password, "ps -eo pid,cmd | grep 'ray start' | grep -v grep || true", timeout=timeout)
    
    text = out or ""
    if "--head" in text:
        return "head"
    if "ray start" in text:
        return "worker"
    return "none"

def start_head(host, username, password, port, timeout=20, dry_run=False):
    """Start the ray cluster in the host machine"""
    cmd = f"ray stop --force || true; ray start --head --port={int(port)} --dashboard-host=0.0.0.0"  # Command for starting a new head after killing anything existing
    if dry_run:
        print(f"[dry-run] {host}: {cmd}")  # If it is dry run, just print it
        return

    exit_code, out, err = run_remote(host, username, password, cmd, timeout=timeout)  # Run the start command
    if exit_code != 0:
        message = (err or out).strip() or "failed to start head"
        raise RuntimeError(message)

def start_worker(host, username, password, head_ip, port, monitor_name, conda_env=None, timeout=20, dry_run=False):
    base_cmd = f"ray stop --force || true; ray start --address='{head_ip}:{int(port)}' "
    cmd = wrap_with_conda_env(base_cmd, conda_env)

    if dry_run:
        warning(f"[dry-run] {host}: {cmd}")
        return

    with status(f"Starting worker {host}..."):
        exit_code, out, err = run_remote(host, username, password, cmd, timeout=timeout)

    if exit_code != 0:
        message = (err or out).strip() or f"failed to start worker {host}"
        error(f"Worker {host} failed to start")
        raise RuntimeError(message)

    success(f"Started worker {host}")

def setup_head(head_ip, username, password, port, dry_run=False):
    info(f"Connecting to head {head_ip}")

    if dry_run:
        warning("[dry-run] Would start head node")
        start_head(head_ip, username, password, port=port, dry_run=True)
        return

    require_ray(head_ip, username, password)
    state = head_state(head_ip, username, password)

    if state == "head":
        success("Head already running")
        return

    if state == "worker":
        warning("Head machine running as worker, restarting as head")
    else:
        info("Head not running, starting fresh")

    with status(f"Starting head {head_ip}..."):
        start_head(head_ip, username, password, port=port, dry_run=dry_run)

    success(f"Started head {head_ip}")

def setup_workers(workers, default_user, default_password, head_ip, port, dry_run=False):
    """Setup the workers for the given head IP"""
    for worker in workers:
        host = normalize(worker.get("ip-address"))
        if not host or host in {head_ip, GRID_HEAD_IP}:  # If the worker is the head or the grid head do not start anything
            continue

        user = normalize(worker.get("username")) or default_user
        password = normalize(worker.get("password")) or default_password
        monitor_name = normalize(worker.get("monitor-name"))
        conda_env = normalize(worker.get("env"))
        try:
            if dry_run:
                start_worker(host, user, password, head_ip=head_ip, port=port, monitor_name=monitor_name, conda_env=conda_env, dry_run=True)
                continue
            require_ray(host, user, password, conda_env=conda_env)  # Check if the worker has ray
            start_worker(host, user, password, head_ip=head_ip, port=port, monitor_name=monitor_name, conda_env=conda_env, dry_run=False)  # Start the worker
        except Exception as exc:
            error(f"{host}: {exc}")  # If there is an issue, print the exception and continue
            continue

def parse_args():
    env_defaults = load_env_defaults()
    parser = argparse.ArgumentParser(description="Configure Ray head and workers")
    parser.add_argument("--username", help="SSH username", default=env_defaults.get("USERNAME"))
    parser.add_argument("--password", help="SSH password", default=env_defaults.get("PASSWORD"))
    parser.add_argument("--workers-file", default=str(WORKER_FILE))
    parser.add_argument("--head-ip", default=DEFAULT_HEAD_IP)
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--dry-run", action="store_true", help="Print commands without executing")
    args = parser.parse_args()

    missing = [field for field in ("username", "password") if not getattr(args, field)]
    if missing:
        parser.error(f"Missing required credentials: {', '.join(missing)}. Add CLI arguments or update {ENV_FILE.name}.")

    return args

def main():
    args = parse_args()
    workers = load_workers(Path(args.workers_file))
    try:
        setup_head(args.head_ip, args.username, args.password, port=args.port, dry_run=args.dry_run)  # Setup the cluster head
        setup_workers(workers, args.username, args.password, head_ip=args.head_ip, port=args.port, dry_run=args.dry_run)  # Setup all the workers
    except RuntimeError as exc:
        sys.exit(str(exc))

if __name__ == "__main__":
    main()
