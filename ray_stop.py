import argparse
import csv
import os
import yaml
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
import paramiko
from ping_workers import WORKER_FILE, ENV_FILE, load_env_defaults, load_workers, normalize
from ray_setup import wrap_with_conda_env
from ray_diagnosis import short_text, run_remote
from rich.console import Console
from rich.table import Table

CONFIG_FILE = Path(__file__).with_name("config.yaml")
console = Console()

def get_grid_head_ip():
    """Get GRID_HEAD_IP from config.yaml."""
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(f"Config file not found: {CONFIG_FILE}")
    
    with CONFIG_FILE.open(encoding="utf-8") as f:
        config = yaml.safe_load(f)
    
    return config.get("GRID_HEAD_IP")

def parse_worker_ids(raw: str | None) -> set[str]:
    """
    Parse --workers value into a normalized, case-insensitive selector set.
    Accepts comma-separated values; ignores empty segments.
    """
    text = normalize(raw)
    if not text:
        return set()
    parts = [normalize(p) for p in text.split(",")]
    return {p.lower() for p in parts if p}

def match_workers(worker: dict, selectors_lc: set[str]) -> bool:
    """Return True if worker matches any selector by ip-address, hostname, or monitor-name."""
    if not selectors_lc:
        return True
    ip_addr = normalize(worker.get("ip-address")).lower()
    hostname = normalize(worker.get("hostname")).lower()
    monitor = normalize(worker.get("monitor-name")).lower()
    return bool({ip_addr, hostname, monitor} & selectors_lc)

def parse_args():
    """Parse args for ray_stop (includes --workers selector list)."""
    env_defaults = load_env_defaults()
    parser = argparse.ArgumentParser(description="Stop Ray on workers listed in workers.csv")
    parser.add_argument("--username", help="SSH username", default=env_defaults.get("USERNAME"))
    parser.add_argument("--password", help="SSH password", default=env_defaults.get("PASSWORD"))
    parser.add_argument("--workers", help=("Comma-separated selectors to target specific workers by IP address, hostname, or monitor-name.\nExample: --workers 136.244.224.165,rat,NL214-Lin11171"), default="")
    args = parser.parse_args()

    missing = [field for field in ("username", "password") if not getattr(args, field)]
    if missing:
        parser.error(f"Missing required credentials: {', '.join(missing)}. Add CLI arguments or update {ENV_FILE.name}.")

    return args

def stop_ray(host, username, password, conda_env=None, timeout=20):
    """Connect to the hosts and stop the Ray cluster."""
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        ssh.connect(host, username=username, password=password, timeout=timeout, auth_timeout=timeout, banner_timeout=timeout)

        code, out, err = run_remote(ssh, "echo ok", timeout=10)
        if code != 0 or out.strip() != "ok":
            return "failed", short_text((err or out).strip() or "Unexpected SSH output", 160)

        # If an env is specified, try inside conda first, then fall back.
        cmd = "ray stop --force || true"
        env = normalize(conda_env)
        if env:
            cmd = f"{wrap_with_conda_env('ray stop --force', env)} || {cmd}"

        stop_code, stop_out, stop_err = run_remote(ssh, cmd, timeout=timeout)

        # Treat SSH success + command executed as ok; ray may not be installed everywhere.
        details = (stop_err or stop_out).strip()
        if stop_code == 0:
            return "ok", short_text(details or "stopped", 160)

        # If ray isn't available, cmd may still be ok due to `|| true`; otherwise capture output.
        if "command not found" in details.lower() or "no such file" in details.lower():
            return "ok", short_text(details or "ray not found", 160)

        return "failed", short_text(details or f"ray stop failed (exit {stop_code})", 160)

    except paramiko.AuthenticationException:
        return "failed", "Authentication failed."
    except paramiko.SSHException as exc:
        return "failed", short_text(f"SSH connection failed: {exc}", 160)
    except Exception as exc:
        return "failed", short_text(f"Error: {exc}", 160)
    finally:
        try:
            ssh.close()
        except Exception:
            pass

def stop_one_worker(worker: dict, default_username: str, default_password: str, grid_head_ip: str) -> dict:
    """Picklable task: stop Ray on a single worker."""
    room = normalize(worker.get("room"))
    hostname = normalize(worker.get("hostname"))
    host_ip = normalize(worker.get("ip-address"))
    monitor = normalize(worker.get("monitor-name"))
    username = normalize(worker.get("username")) or default_username
    password = normalize(worker.get("password")) or default_password
    conda_env = normalize(worker.get("env"))

    if not host_ip or host_ip == grid_head_ip:
        return {
            "room": room,
            "hostname": hostname,
            "ip": host_ip,
            "monitor": monitor,
            "status": "skipped",
            "details": "skipped",
        }

    status, details = stop_ray(host_ip, username, password, conda_env=conda_env)
    return {
        "room": room,
        "hostname": hostname,
        "ip": host_ip,
        "monitor": monitor,
        "status": status,
        "details": details,
    }

def ray_stop():
    args = parse_args()

    if not WORKER_FILE.exists():
        raise FileNotFoundError(f"CSV not found: {WORKER_FILE}")

    grid_head_ip = get_grid_head_ip()
    selectors_lc = parse_worker_ids(getattr(args, "workers", ""))
    if selectors_lc:
        print(f"Stopping ray on selected workers from {WORKER_FILE} (matched by ip/hostname/monitor-name)")
        print(f"Selectors: {', '.join(sorted(selectors_lc))}")
    else:
        print(f"Stopping ray on workers from {WORKER_FILE}")

    all_workers = load_workers(WORKER_FILE)
    selected_workers = [
        w
        for w in all_workers
        if match_workers(w, selectors_lc) and normalize(w.get("ip-address")) and normalize(w.get("ip-address")) != grid_head_ip
    ]

    if selectors_lc and not selected_workers:
        raise SystemExit("No workers matched --workers selectors. Check workers.csv for ip-address / hostname / monitor-name values.")

    cpu_count = os.cpu_count() or 1
    procs_per_core = 3
    max_procs = max(1, cpu_count * procs_per_core)
    pool_size = min(len(selected_workers), max_procs) if selected_workers else 0

    results_by_idx: dict[int, dict] = {}
    if pool_size:
        with ProcessPoolExecutor(max_workers=pool_size) as executor:
            futures = {
                executor.submit(stop_one_worker, worker, args.username, args.password, grid_head_ip): idx
                for idx, worker in enumerate(selected_workers)
            }
            for future in as_completed(futures):
                idx = futures[future]
                try:
                    results_by_idx[idx] = future.result()
                except Exception as exc:
                    w = selected_workers[idx]
                    results_by_idx[idx] = {
                        "room": normalize(w.get("room")),
                        "hostname": normalize(w.get("hostname")),
                        "ip": normalize(w.get("ip-address")),
                        "monitor": normalize(w.get("monitor-name")),
                        "status": "failed",
                        "details": short_text(f"Worker task crashed: {exc}", 160),
                    }

    results = [results_by_idx[i] for i in range(len(selected_workers))] if selected_workers else []

    counts = {"ok": 0, "failed": 0}
    messages: list[str] = []

    # Rich output table
    table = Table(title=f"Ray stop results ({len(results)} workers)", box=None, show_lines=False)
    table.add_column("ROOM", style="white", no_wrap=True)
    table.add_column("HOSTNAME", style="white")
    table.add_column("IP-ADDRESS", style="white")
    table.add_column("MONITOR", style="white")
    table.add_column("STOPPED", justify="center")
    table.add_column("DETAILS", style="white", no_wrap=True, overflow="ellipsis", max_width=80)

    rows = []
    headers = ["room", "hostname", "ip-address", "monitor-name", "STOPPED"]
    rows.append(headers)

    for r in results:
        status = r["status"]
        if status == "ok":
            counts["ok"] += 1
            stopped = "YES"
            style = "green"
        else:
            counts["failed"] += 1
            stopped = "NO"
            style = "red"

        table.add_row(
            r["room"],
            r["hostname"],
            r["ip"],
            r["monitor"],
            stopped,
            short_text(r.get("details", ""), 120) or "-",
            style=style,
        )
        rows.append([r["room"], r["hostname"], r["ip"], r["monitor"], stopped])

        if status != "ok":
            messages.append(f"{r['hostname'] or r['ip']}: {r.get('details', '')}")

    console.print(table)

    print(f"\n\n[SUMMARY] {counts['ok']} ok, {counts['failed']} failed")
    if messages:
        print("\nError Message:")
        for message in messages:
            print(f"--> {message}")

    temp_dir = Path(__file__).with_name("temp")
    temp_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%y%m%d_%H%M%S")
    log_path = temp_dir / f"{timestamp}_ray_stop.csv"
    with log_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerows(rows)

    print(f"\nSaved log to {log_path}")

if __name__ == "__main__":
    ray_stop()
