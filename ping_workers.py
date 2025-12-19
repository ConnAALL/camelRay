"""
Scan the nodes in workers.csv and check if they are reachable via SSH.

This assumes you are using a computer on the 136.244.224.xxx network.
"""

import argparse
import csv
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
import paramiko
from pathlib import Path
from rich.console import Console
from rich.table import Table

WORKER_FILE = Path(__file__).with_name("workers.csv")
ENV_FILE = Path(__file__).with_name(".env")

def load_env_defaults():
    """Load username/password information from the .env file if it exists."""
    env_data = {}
    if not ENV_FILE.exists():
        return env_data

    with ENV_FILE.open(encoding="utf-8") as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            env_data[key.strip()] = value.strip()

    return env_data

def parse_args():
    """Parse the command-line arguments that are the username and the password of the user"""
    env_defaults = load_env_defaults()
    parser = argparse.ArgumentParser(description="Sweep through all the workers in the workers.csv file to ensure that they are reachable")
    parser.add_argument("--username", help="SSH username", default=env_defaults.get("USERNAME"))
    parser.add_argument("--password", help="SSH password", default=env_defaults.get("PASSWORD"))
    args = parser.parse_args()

    missing = [field for field in ("username", "password") if not getattr(args, field)]
    if missing:
        parser.error(f"Missing required credentials: {', '.join(missing)}. Add CLI arguments or update {ENV_FILE.name}.")

    return args

def load_workers(csv_path):
    """Load the workers in the csv file into the dictionary"""
    with csv_path.open(newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))
    
def normalize(value):
    """Simple pre-processing of the worker information from the workers.csv file"""
    return (value or "").strip()

def process_gpu_output(out):
    text = (out or "").strip()
    if not text:
        return ("", "")
    
    # use split to get everything after 'controller:', if present
    split_key = "controller:"
    if split_key in text:
        text = text.split(split_key, 1)[1].strip()
    
    # use split to remove anything after '(rev', if present
    rev_key = "(rev"
    if rev_key.lower() in text.lower():
        # Find the index in a case-insensitive way and trim
        idx = text.lower().index(rev_key.lower())
        text = text[:idx].strip()
        
    if len(text) > 50:
        text = text[:45] + "..."
    return (out, text)

def ssh_check(host, user, password):
    """Run the SSH check and get hardware information if possible."""
    cores = "unknown"
    gpu = ("unknown", "unknown")
    try:
        # Create the SSH client
        ssh = paramiko.SSHClient()

        # Automatically add the server's host key
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # Connect to the remote server using the given username and password
        ssh.connect(host, username=user, password=password, timeout=5)

        # Run a simple command to check if the SSH connection is established
        stdin, stdout, stderr = ssh.exec_command("echo ok")

        # Read the output
        result = stdout.read().decode().strip()

        # Fetch CPU core count when the ping succeeds
        if result == "ok":
            _, stdout, _ = ssh.exec_command("nproc --all")
            cores_output = stdout.read().decode().strip()
            if cores_output.isdigit():
                cores = cores_output
            _, stdout, _ = ssh.exec_command("lspci | grep -i vga | head -n 1 || true")
            gpu = process_gpu_output(stdout.read().decode().strip())

        # Close the connection
        ssh.close()

        # If the result is 'ok', the connection is successful
        if result == "ok":
            return "ok", "", cores, gpu
        else:
            return "failed", f"Unexpected output: {result}", cores, gpu

    except paramiko.AuthenticationException:
        return "failed", "Authentication failed.", cores, gpu
    except paramiko.SSHException as e:
        return "failed", f"SSH connection failed: {str(e)}", cores, gpu
    except Exception as e:
        return "failed", f"Error: {str(e)}", cores, gpu

def ping_one_worker(worker: dict, default_username: str, default_password: str) -> dict:
    """
    Worker function (picklable) to run in a separate process.
    Returns a dict with normalized fields + ssh check outputs.
    """
    room = normalize(worker.get("room"))
    hostname = normalize(worker.get("hostname"))
    host_ip = normalize(worker.get("ip-address"))
    monitor = normalize(worker.get("monitor-name"))
    username = normalize(worker.get("username")) or default_username
    password = normalize(worker.get("password")) or default_password

    status, details, cores, gpu = ssh_check(host_ip, username, password)
    gpu_raw, gpu_short = gpu if isinstance(gpu, tuple) and len(gpu) == 2 else ("", "")
    return {
        "room": room,
        "hostname": hostname,
        "ip": host_ip,
        "monitor": monitor,
        "status": status,
        "details": details,
        "cores": cores,
        "gpu_raw": gpu_raw,
        "gpu_short": gpu_short,
    }

def print_results_table(results: list[dict]):
    """Print results as a Rich table."""
    table = Table(title=f"Ping results ({len(results)} workers)")
    table.add_column("ROOM", style="cyan", no_wrap=True)
    table.add_column("HOSTNAME", style="white")
    table.add_column("IP-ADDRESS", style="white")
    table.add_column("MONITOR", style="white")
    table.add_column("CORES", justify="right")
    table.add_column("GPU", style="white")
    table.add_column("SUCCESS", justify="center")

    for r in results:
        success = "YES" if r["status"] == "ok" else "NO"
        style = "green" if r["status"] == "ok" else "red"
        table.add_row(
            r["room"],
            r["hostname"],
            r["ip"],
            r["monitor"],
            str(r["cores"]),
            r["gpu_short"],
            success,
            style=style,
        )

    Console().print(table)

def ping_workers():
    """Go through each available worker and ping them to see if they are available or not"""
    args = parse_args()

    # If the workers.csv does not exist, raise an error
    if not WORKER_FILE.exists():
        raise FileNotFoundError(f"CSV not found: {WORKER_FILE}")

    all_workers = load_workers(WORKER_FILE)

    cpu_count = os.cpu_count() or 1
    procs_per_core = 3
    max_procs = max(1, cpu_count * procs_per_core)
    pool_size = min(len(all_workers), max_procs) if all_workers else 0

    results_by_idx: dict[int, dict] = {}
    if pool_size == 0:
        results: list[dict] = []
    else:
        with ProcessPoolExecutor(max_workers=pool_size) as executor:
            futures = {
                executor.submit(ping_one_worker, worker, args.username, args.password): idx
                for idx, worker in enumerate(all_workers)
            }
            for future in as_completed(futures):
                idx = futures[future]
                try:
                    results_by_idx[idx] = future.result()
                except Exception as exc:
                    w = all_workers[idx]
                    results_by_idx[idx] = {
                        "room": normalize(w.get("room")),
                        "hostname": normalize(w.get("hostname")),
                        "ip": normalize(w.get("ip-address")),
                        "monitor": normalize(w.get("monitor-name")),
                        "status": "failed",
                        "details": f"Worker task crashed: {exc}",
                        "cores": "unknown",
                        "gpu_raw": "",
                        "gpu_short": "",
                    }

        results = [results_by_idx[i] for i in range(len(all_workers))]

    print_results_table(results)

    counts = {"ok": 0, "failed": 0}
    messages = []

    for r in results:
        counts[r["status"]] += 1
        if r["details"]:
            messages.append(f"{r['hostname'] or r['ip']}: {r['details']}")

    print(f"\n\n[SUMMARY] {counts['ok']} ok, {counts['failed']} failed")
    
    if messages:
        print("\nError Message:")
        for message in messages:
            print(f"--> {message}")

if __name__ == "__main__":
    ping_workers()
