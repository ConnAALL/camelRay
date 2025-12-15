"""
Scan the nodes in workers.csv and check if they are reachable via SSH.

This assumes you are using a computer on the 136.244.224.xxx network.
"""

import argparse
import csv
import paramiko
from datetime import datetime
from pathlib import Path

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
        return ""
    
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

def ping_workers():
    """Go through each available worker and ping them to see if they are available or not"""
    args = parse_args()

    # If the workers.csv does not exist, raise an error
    if not WORKER_FILE.exists():
        raise(f"CSV not found: {WORKER_FILE}")

    print(f"Scanning workers from {WORKER_FILE}")

    counts = {"ok": 0, "failed": 0}  # Success / Non-Success counts
    messages = []  # Error details from the runs
    rows = []
    headers = ["room","hostname","ip-address","monitor-name","cores","gpu","success"]
    print(f"{'ROOM':<5} {'HOSTNAME':<30} {'IP-ADDRESS':<20} {'MONITOR':<15} {'CORES':<10} {'GPU':<50} {'SUCCESS'}")

    for worker in load_workers(WORKER_FILE):
        # Get the worker details
        room = normalize(worker.get("room"))
        hostname = normalize(worker.get("hostname"))
        host_ip = normalize(worker.get("ip-address"))
        monitor = normalize(worker.get("monitor-name"))
        username = normalize(worker.get("username")) or args.username
        password = normalize(worker.get("password")) or args.password

        # Ping the ssh in the specific host
        status, details, cores, gpu = ssh_check(host_ip, username, password)
        
        # Keep track of how we had been doing
        counts[status] += 1
        success = {"ok": "YES", "failed": "NO"}.get(status)
        
        print(f"{room:<5} {hostname:<30} {host_ip:<20} {monitor:<15} {cores:<10} {gpu[1]:<50} {success}")
        rows.append([room,hostname,host_ip,monitor,cores,gpu[0],success])
        
        # If it errored, add the error message
        if details:
            messages.append(f"{hostname or host_ip}: {details}")

    print(f"\n\n[SUMMARY] {counts['ok']} ok, {counts['failed']} failed, ")
    
    if messages:
        print("\nError Message:")
        for message in messages:
            print(f"--> {message}")

    temp_dir = Path(__file__).with_name("temp")
    temp_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%y%m%d_%H%M%S")
    log_path = temp_dir / f"{timestamp}_log.csv"
    with log_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerows(rows)

if __name__ == "__main__":
    ping_workers()
