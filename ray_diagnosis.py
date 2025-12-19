"""
Script that runs a diagnosis of an existing Ray cluster.

It goes through each worker in the workers.csv file if:
 - They are reachable via SSH
 - They have Ray installed (and the ray version)
 - They are running Ray
 - They are running as a head node or a worker node
 
"""

import os
import shlex
from concurrent.futures import ProcessPoolExecutor, as_completed
import paramiko
from ping_workers import WORKER_FILE, load_workers, normalize, parse_args
from ray_setup import wrap_with_conda_env
from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

def short_text(value, max_len):
    """Shorten the text to the maximum length"""
    text = (value or "").strip()
    if len(text) <= max_len:
        return text
    if max_len <= 3:
        return text[:max_len]
    return text[: max_len - 3] + "..."

def run_remote(ssh, command, timeout=10):
    """Run a command on the remote host"""
    remote_cmd = f"bash -lc {shlex.quote(command)}"
    _, stdout, stderr = ssh.exec_command(remote_cmd, get_pty=False, timeout=timeout)
    out = stdout.read().decode(errors="replace")
    err = stderr.read().decode(errors="replace")
    exit_code = stdout.channel.recv_exit_status()
    return exit_code, out, err

def run_diagnosis(host, username, password, conda_env):
    """Run a diagnosis test on the specific host"""
    # The default result values
    result = {"ssh": "NO", "ray_installed": "unknown", "ray_version": "", "ray_running": "unknown", "role": "unknown", "details": ""}

    ssh = paramiko.SSHClient()  # Create the SSH connection
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(host, username=username, password=password, timeout=5, auth_timeout=5, banner_timeout=5)
        code, out, err = run_remote(ssh, "echo ok", timeout=5)  # Check if the connection is established
        if code != 0 or out.strip() != "ok":
            result["details"] = short_text((err or out).strip() or "Unexpected SSH output", 160)
            return result
        result["ssh"] = "YES"

        # Check the different commands to see the ray version
        ray_check_cmd = wrap_with_conda_env(
            (
                "command -v ray >/dev/null 2>&1 && "
                "(ray --version 2>/dev/null || python3 -c 'import ray; print(ray.__version__)' 2>/dev/null || python -c 'import ray; print(ray.__version__)' 2>/dev/null) "
                "|| echo NO_RAY"
            ),
            conda_env)
        ray_exit, out, err = run_remote(ssh, ray_check_cmd, timeout=10)
        ray_out = (out or "").strip()
        if "NO_RAY" in ray_out:
            result["ray_installed"] = "NO"  # If no output at all, return NO
        elif ray_exit == 0 and ray_out:  # If there is output, return the version
            result["ray_installed"] = "YES"
            result["ray_version"] = short_text(ray_out.split()[-1], 18)
        else:
            result["ray_installed"] = "unknown"
            if not result["details"]:
                result["details"] = short_text((err or ray_out).strip() or "Ray check failed", 160)

        _, raylet_out, _ = run_remote(ssh, "ps -eo args | grep -m1 '[r]aylet' || true", timeout=10)
        _, gcs_out, _ = run_remote(ssh, "ps -eo args | grep -m1 '[g]cs_server' || true", timeout=10)
        raylet_cmd = (raylet_out or "").strip()
        gcs_cmd = (gcs_out or "").strip()

        if gcs_cmd:
            result["ray_running"] = "YES"
            result["role"] = "head"
        elif raylet_cmd:
            result["ray_running"] = "YES"
            result["role"] = "worker"
        else:
            result["ray_running"] = "NO"
            result["role"] = "none"
        return result

    except paramiko.AuthenticationException:
        result["details"] = "Authentication failed."
        return result
    except paramiko.SSHException as exc:
        result["details"] = short_text(f"SSH connection failed: {exc}", 160)
        return result
    except Exception as exc:
        result["details"] = short_text(f"Error: {exc}", 160)
        return result
    finally:
        try:
            ssh.close()  # Close the connection
        except Exception:
            pass

def diagnosis_one_worker(worker: dict, default_username: str, default_password: str) -> dict:
    """Picklable worker function to run diagnosis for a single worker in a separate process."""
    room = normalize(worker.get("room"))
    hostname = normalize(worker.get("hostname"))
    host_ip = normalize(worker.get("ip-address"))
    monitor = normalize(worker.get("monitor-name"))
    username = normalize(worker.get("username")) or default_username
    password = normalize(worker.get("password")) or default_password
    conda_env = normalize(worker.get("env"))

    diag = run_diagnosis(host_ip, username, password, conda_env)
    return {
        "room": room,
        "hostname": hostname,
        "ip": host_ip,
        "monitor": monitor,
        "ssh": diag["ssh"],
        "ray_installed": diag["ray_installed"],
        "ray_version": diag["ray_version"],
        "ray_running": diag["ray_running"],
        "role": diag["role"],
        "details": diag["details"],
    }

def print_results_table(results: list[dict]):
    """Print results as a Rich table."""
    table = Table(title=None)
    table.add_column("ROOM", style="cyan", no_wrap=True)
    table.add_column("HOSTNAME", style="white")
    table.add_column("IP-ADDRESS", style="white")
    table.add_column("MONITOR", style="white")
    table.add_column("SSH", justify="center")
    table.add_column("RAY", justify="center")
    table.add_column("RAY_VERSION", style="white")
    table.add_column("RAY_RUNNING", justify="center")
    table.add_column("ROLE", style="white")

    for r in results:
        ver_disp = short_text(r["ray_version"] or "-", 12)

        if r["ray_running"] == "unknown":
            style = "yellow"
        elif r["ray_running"] == "NO":
            style = "red"
        elif r["ssh"] != "YES":
            style = "red"
        else:
            style = "green"
        table.add_row(
            r["room"],
            r["hostname"],
            r["ip"],
            r["monitor"],
            r["ssh"],
            r["ray_installed"],
            ver_disp,
            r["ray_running"],
            r["role"],
            style=style,
        )

    Console().print(table)

def ray_diagnosis():
    """General diagnosis test"""
    args = parse_args()

    if not WORKER_FILE.exists():
        raise FileNotFoundError(f"CSV not found: {WORKER_FILE}")

    workers = load_workers(WORKER_FILE)

    cpu_count = os.cpu_count() or 1
    procs_per_core = 3
    max_procs = max(1, cpu_count * procs_per_core)
    pool_size = min(len(workers), max_procs) if workers else 0

    results_by_idx: dict[int, dict] = {}
    if pool_size == 0:
        results: list[dict] = []
    else:
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold]Diagnosing workers[/bold]"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(),
            console=Console(),
            transient=True,
        ) as progress:
            task_id = progress.add_task("diag", total=len(workers))
            with ProcessPoolExecutor(max_workers=pool_size) as executor:
                futures = {
                    executor.submit(diagnosis_one_worker, worker, args.username, args.password): idx
                    for idx, worker in enumerate(workers)
                }
                for future in as_completed(futures):
                    idx = futures[future]
                    try:
                        results_by_idx[idx] = future.result()
                    except Exception as exc:
                        w = workers[idx]
                        results_by_idx[idx] = {
                            "room": normalize(w.get("room")),
                            "hostname": normalize(w.get("hostname")),
                            "ip": normalize(w.get("ip-address")),
                            "monitor": normalize(w.get("monitor-name")),
                            "ssh": "NO",
                            "ray_installed": "unknown",
                            "ray_version": "",
                            "ray_running": "unknown",
                            "role": "unknown",
                            "details": f"Worker task crashed: {exc}",
                        }
                    progress.advance(task_id, 1)

            results = [results_by_idx[i] for i in range(len(workers))]

    print_results_table(results)

    counts = {"YES": 0, "NO": 0, "unknown": 0}
    messages = []

    for r in results:
        ray_state = r.get("ray_running", "unknown")
        if ray_state not in {"YES", "NO", "unknown"}:
            ray_state = "unknown"
        counts[ray_state] += 1
        if r["details"]:
            messages.append(f"{r['hostname'] or r['ip']}: {r['details']}")

    print(f"\n\n[SUMMARY] {counts['YES']} running Ray, {counts['NO']} not running Ray, {counts['unknown']} unknown")
    if messages:
        print("\nError Message:")
        for message in messages:
            print(f"--> {message}")

if __name__ == "__main__":
    ray_diagnosis()
