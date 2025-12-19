"""
Setup the ray cluster on the workers. 

It goes through each worker in the workers.csv file and starts the ray cluster on the workers.
It uses the SSH credentials in .env file and the config.yaml file to setup the ray cluster with the head node.
By default, it does not use the main grid computer as a worker to not overload the jump-host.

If you would like to start the ray cluster on the machines that are not running ray, you can use the --prune option.
Otherwise, the script will stop any existing ray processes and start a fresh cluster.
"""

import argparse
import os
import paramiko
import shlex
import sys
import yaml
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from ping_workers import load_env_defaults, normalize, load_workers, WORKER_FILE, ENV_FILE
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.theme import Theme
from rich.markup import escape
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

def short_text(value: str, max_len: int) -> str:
    text = (value or "").strip()
    if len(text) <= max_len:
        return text
    if max_len <= 3:
        return text[:max_len]
    return text[: max_len - 3] + "..."

CONFIG_FILE = Path(__file__).with_name("config.yaml")

def load_config():
    """Load configuration from config.yaml file."""
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(f"Config file not found: {CONFIG_FILE}")
    
    with CONFIG_FILE.open(encoding="utf-8") as f:
        config = yaml.safe_load(f)
    
    return config

def get_default_head_ip():
    """Get DEFAULT_HEAD_IP from config.yaml."""
    config = load_config()
    return config.get("DEFAULT_HEAD_IP")

def get_grid_head_ip():
    """Get GRID_HEAD_IP from config.yaml."""
    config = load_config()
    return config.get("GRID_HEAD_IP")

DEFAULT_HEAD_IP = get_default_head_ip()
GRID_HEAD_IP = get_grid_head_ip()

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

def ray_process_state(host, username, password, timeout=10):
    """
    Return whether Ray appears to be running on the machine.

    We detect the common Ray processes instead of relying on `ray start` history.
    Returns one of: "head", "worker", "none".
    """
    _, raylet_out, _ = run_remote(host, username, password, "ps -eo args | grep -m1 '[r]aylet' || true", timeout=timeout)
    _, gcs_out, _ = run_remote(host, username, password, "ps -eo args | grep -m1 '[g]cs_server' || true", timeout=timeout)
    raylet_cmd = (raylet_out or "").strip()
    gcs_cmd = (gcs_out or "").strip()
    if gcs_cmd:
        return "head"
    if raylet_cmd:
        return "worker"
    return "none"

def head_state(host, username, password, timeout=10):
    """Determine the state of the head node including whether it is running ray as a worker or host, or not running ray at all"""
    _, out, _err = run_remote(host, username, password, "ps -eo pid,cmd | grep 'ray start' | grep -v grep || true", timeout=timeout)
    
    text = out or ""
    if "--head" in text:
        return "head"
    if "ray start" in text:
        return "worker"
    return "none"

def start_head(host, username, password, port, timeout=20, prune=False):
    """Start the ray cluster in the host machine"""
    if prune:
        cmd = f"ray start --head --port={int(port)} --dashboard-host=0.0.0.0"
    else:
        cmd = f"ray stop --force || true; ray start --head --port={int(port)} --dashboard-host=0.0.0.0"  # Command for starting a new head after killing anything existing

    exit_code, out, err = run_remote(host, username, password, cmd, timeout=timeout)  # Run the start command
    if exit_code != 0:
        message = (err or out).strip() or "failed to start head"
        raise RuntimeError(message)

def start_worker(host, username, password, head_ip, port, conda_env=None, timeout=20, prune=False):
    if prune:
        base_cmd = f"ray start --address='{head_ip}:{int(port)}' "
    else:
        base_cmd = f"ray stop --force || true; ray start --address='{head_ip}:{int(port)}' "
    cmd = wrap_with_conda_env(base_cmd, conda_env)

    with status(f"Starting worker {host}..."):
        exit_code, out, err = run_remote(host, username, password, cmd, timeout=timeout)

    if exit_code != 0:
        message = (err or out).strip() or f"failed to start worker {host}"
        error(f"Worker {host} failed to start")
        raise RuntimeError(message)

    success(f"Started worker {host}")

def setup_head(head_ip, username, password, port, prune=False):
    info(f"Connecting to head {head_ip}")

    if prune:
        running = ray_process_state(head_ip, username, password)
        if running == "head":
            success("Head already running")
            return
        if running == "worker":
            warning("Head machine already running Ray as a worker; skipping due to --prune")
            return
        info("Head not running, starting fresh (--prune)")
        require_ray(head_ip, username, password)
        with status(f"Starting head {head_ip}..."):
            start_head(head_ip, username, password, port=port, prune=True)
        success(f"Started head {head_ip}")
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
        start_head(head_ip, username, password, port=port, prune=False)

    success(f"Started head {head_ip}")

def start_worker_quiet(host, username, password, *, head_ip, port, conda_env=None, timeout=20, prune=False):
    """Start a Ray worker without progress/spinner output (used from multiprocessing workers)."""
    if prune:
        base_cmd = f"ray start --address='{head_ip}:{int(port)}' "
    else:
        base_cmd = f"ray stop --force || true; ray start --address='{head_ip}:{int(port)}' "
    cmd = wrap_with_conda_env(base_cmd, conda_env)
    exit_code, out, err = run_remote(host, username, password, cmd, timeout=timeout)
    if exit_code != 0:
        message = (err or out).strip() or f"failed to start worker {host}"
        raise RuntimeError(message)

def setup_worker_task(worker: dict, default_user: str, default_password: str, head_ip: str, port: int, prune: bool) -> dict:
    """Picklable worker task to start Ray on a single worker."""
    room = normalize(worker.get("room"))
    host = normalize(worker.get("ip-address"))
    hostname = normalize(worker.get("hostname"))
    monitor = normalize(worker.get("monitor-name"))
    user = normalize(worker.get("username")) or default_user
    password = normalize(worker.get("password")) or default_password
    conda_env = normalize(worker.get("env"))

    try:
        if prune:
            running = ray_process_state(host, user, password)
            if running != "none":
                return {"room": room, "host": host, "hostname": hostname, "monitor": monitor, "status": "skipped", "details": f"Ray already running ({running}); skipped due to --prune"}

        require_ray(host, user, password, conda_env=conda_env)
        start_worker_quiet(host, user, password, head_ip=head_ip, port=port, conda_env=conda_env, prune=prune)
        return {"room": room, "host": host, "hostname": hostname, "monitor": monitor, "status": "ok", "details": ""}
    except Exception as exc:
        return {"room": room, "host": host, "hostname": hostname, "monitor": monitor, "status": "failed", "details": str(exc)}

def setup_workers(workers, default_user, default_password, head_ip, port, prune=False):
    """Setup the workers for the given head IP"""
    # NOTE: This function is parallelized; avoid printing from subprocesses.
    def eligible(worker: dict) -> bool:
        host = normalize(worker.get("ip-address"))
        return bool(host) and host not in {head_ip, GRID_HEAD_IP}

    targets = [w for w in workers if eligible(w)]
    if not targets:
        warning("No eligible workers found (after excluding head + grid head)")
        return

    cpu_count = os.cpu_count() or 1
    procs_per_core = 3
    max_procs = max(1, cpu_count * procs_per_core)
    pool_size = min(len(targets), max_procs)

    results_by_idx: dict[int, dict] = {}
    task_ids: dict[int, int] = {}
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold]{task.fields[host]}[/bold]"),
        TextColumn("{task.description}"),
        console=console,
        transient=False,
    ) as progress:
        for idx, w in enumerate(targets):
            room = normalize(w.get("room"))
            host = normalize(w.get("ip-address"))
            hostname = normalize(w.get("hostname"))
            monitor = normalize(w.get("monitor-name"))
            base_label = monitor or hostname or host
            label = escape(f"[{room}] {base_label}") if room else base_label
            task_ids[idx] = progress.add_task("queued", total=1, host=label)

        with ProcessPoolExecutor(max_workers=pool_size) as executor:
            futures = {}
            for idx, w in enumerate(targets):
                progress.update(task_ids[idx], description="running")
                futures[executor.submit(setup_worker_task, w, default_user, default_password, head_ip, port, prune)] = idx

            for future in as_completed(futures):
                idx = futures[future]
                try:
                    results_by_idx[idx] = future.result()
                except Exception as exc:
                    w = targets[idx]
                    room = normalize(w.get("room"))
                    results_by_idx[idx] = {
                        "room": room,
                        "host": normalize(w.get("ip-address")),
                        "hostname": normalize(w.get("hostname")),
                        "monitor": normalize(w.get("monitor-name")),
                        "status": "failed",
                        "details": f"Worker task crashed: {exc}",
                    }

                r = results_by_idx[idx]
                status = r["status"]
                details = short_text(r.get("details", ""), 120)
                if status == "ok":
                    desc = "[green]ok[/green]"
                elif status == "skipped":
                    desc = f"[yellow]skipped[/yellow] {details}".rstrip()
                else:
                    desc = f"[red]failed[/red] {details}".rstrip()
                progress.update(task_ids[idx], description=desc, completed=1)

    results = [results_by_idx[i] for i in range(len(targets))]
    counts = {"ok": 0, "failed": 0, "skipped": 0}
    messages: list[str] = []
    for r in results:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
        if r.get("details") and r["status"] == "failed":
            label = r.get("monitor") or r.get("hostname") or r.get("host")
            room = r.get("room") or ""
            prefix = f"[{room}] " if room else ""
            messages.append(f"{prefix}{label} ({r['host']}): {r['details']}")

    info(f"[SUMMARY] {counts.get('ok', 0)} ok, {counts.get('skipped', 0)} skipped, {counts.get('failed', 0)} failed")
    if messages:
        error("Error Message:")
        for message in messages:
            error(f"--> {message}")

def parse_args():
    env_defaults = load_env_defaults()
    parser = argparse.ArgumentParser(description="Configure Ray head and workers")
    parser.add_argument("--username", help="SSH username", default=env_defaults.get("USERNAME"))
    parser.add_argument("--password", help="SSH password", default=env_defaults.get("PASSWORD"))
    parser.add_argument("--workers-file", default=str(WORKER_FILE))
    parser.add_argument("--head-ip", default=DEFAULT_HEAD_IP)
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--prune", action="store_true", help="Only start Ray where it is not already running (do not stop existing Ray processes)")
    args = parser.parse_args()

    missing = [field for field in ("username", "password") if not getattr(args, field)]
    if missing:
        parser.error(f"Missing required credentials: {', '.join(missing)}. Add CLI arguments or update {ENV_FILE.name}.")

    return args

def main():
    args = parse_args()
    workers = load_workers(Path(args.workers_file))
    try:
        setup_head(args.head_ip, args.username, args.password, port=args.port, prune=args.prune)  # Setup the cluster head
        setup_workers(workers, args.username, args.password, head_ip=args.head_ip, port=args.port, prune=args.prune)  # Setup all the workers
    except RuntimeError as exc:
        sys.exit(str(exc))

if __name__ == "__main__":
    main()
