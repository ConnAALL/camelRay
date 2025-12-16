import csv
import shlex
from datetime import datetime
from pathlib import Path
import paramiko
from ping_workers import WORKER_FILE, load_workers, normalize, parse_args
from ray_setup import wrap_with_conda_env, run_remote

def short_text(value, max_len):
    text = (value or "").strip()
    if len(text) <= max_len:
        return text
    if max_len <= 3:
        return text[:max_len]
    return text[: max_len - 3] + "..."

def run_remote(ssh, command, timeout=10):
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

def ray_diagnosis():
    """General diagnosis test"""
    args = parse_args()

    if not WORKER_FILE.exists():
        raise FileNotFoundError(f"CSV not found: {WORKER_FILE}")

    print(f"Scanning workers from {WORKER_FILE}")

    counts = {"ok": 0, "failed": 0}
    messages = []
    rows = []

    headers = ["room", "hostname", "ip-address", "monitor-name", "SSH-PING", "RAY-EXISTS", "RAY_VERSION", "RAY_RUNNING", "ROLE"]
    rows.append(headers)

    print(f"{'ROOM':<5} {'HOSTNAME':<30} {'IP-ADDRESS':<18} {'MONITOR':<15} {'SSH-PING':<8} {'RAY-EXISTS':<10} {'RAY_VERSION':<12} {'RAY_RUNNING':<11} {'ROLE':<8}")

    for worker in load_workers(WORKER_FILE):  # Run the test for each worker
        room = normalize(worker.get("room"))
        hostname = normalize(worker.get("hostname"))
        host_ip = normalize(worker.get("ip-address"))
        monitor = normalize(worker.get("monitor-name"))
        username = normalize(worker.get("username")) or args.username
        password = normalize(worker.get("password")) or args.password
        conda_env = normalize(worker.get("env"))

        diag = run_diagnosis(host_ip, username, password, conda_env)

        status = "ok" if diag["ssh"] == "YES" else "failed"
        counts[status] += 1

        ver_disp = short_text(diag["ray_version"] or "-", 12)

        print(f"{room:<5} {hostname:<30} {host_ip:<18} {monitor:<15} {diag['ssh']:<8} {diag['ray_installed']:<10} {ver_disp:<12} {diag['ray_running']:<11} {diag['role']:<8}")
        rows.append([room, hostname, host_ip, monitor, diag["ssh"], diag["ray_installed"], diag["ray_version"], diag["ray_running"], diag["role"]])

        if diag["details"]:
            messages.append(f"{hostname or host_ip}: {diag['details']}")

    print(f"\n\n[SUMMARY] {counts['ok']} ok, {counts['failed']} failed")
    if messages:
        print("\nError Message:")
        for message in messages:
            print(f"--> {message}")

    temp_dir = Path(__file__).with_name("temp")
    temp_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%y%m%d_%H%M%S")
    log_path = temp_dir / f"{timestamp}_ray_diagnosis.csv"
    with log_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerows(rows)

    print(f"\nSaved log to {log_path}")

if __name__ == "__main__":
    ray_diagnosis()
