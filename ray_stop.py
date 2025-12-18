import csv
from datetime import datetime
from pathlib import Path
import paramiko
from ping_workers import WORKER_FILE, load_workers, normalize, parse_args
from ray_setup import GRID_HEAD_IP, wrap_with_conda_env
from ray_diagnosis import short_text, run_remote

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

def ray_stop():
    args = parse_args()

    if not WORKER_FILE.exists():
        raise FileNotFoundError(f"CSV not found: {WORKER_FILE}")

    print(f"Stopping ray on workers from {WORKER_FILE}")

    counts = {"ok": 0, "failed": 0}
    messages = []

    rows = []
    headers = ["room", "hostname", "ip-address", "monitor-name", "STOPPED", "details"]
    rows.append(headers)

    print(f"{'ROOM':<5} {'HOSTNAME':<30} {'IP-ADDRESS':<18} {'MONITOR':<15} {'STOPPED':<8} DETAILS")

    for worker in load_workers(WORKER_FILE):
        room = normalize(worker.get("room"))
        hostname = normalize(worker.get("hostname"))
        host_ip = normalize(worker.get("ip-address"))
        monitor = normalize(worker.get("monitor-name"))

        if not host_ip or host_ip == GRID_HEAD_IP:
            continue

        username = normalize(worker.get("username")) or args.username
        password = normalize(worker.get("password")) or args.password
        conda_env = normalize(worker.get("env"))

        status, details = stop_ray(host_ip, username, password, conda_env=conda_env)
        counts[status] += 1

        stopped = "YES" if status == "ok" else "NO"
        print(f"{room:<5} {hostname:<30} {host_ip:<18} {monitor:<15} {stopped:<8} {details}")
        rows.append([room, hostname, host_ip, monitor, stopped, details])

        if status != "ok":
            messages.append(f"{hostname or host_ip}: {details}")

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
