"""
Scan the nodes in workers.csv and check if they are reachable via SSH.

This assumes you are using a computer on the 136.244.224.xxx network.
"""

import argparse
import csv
import shlex
import shutil
import subprocess
from pathlib import Path

WORKER_FILE = Path(__file__).with_name("workers.csv")

def parse_args():
    """Parse the command-line arguments that are the username and the password of the user"""
    parser = argparse.ArgumentParser(description="Sweep through all the workers in the workers.csv file to ensure that they are reachable")
    parser.add_argument("--username", help="SSH username", required=True)
    parser.add_argument("--password", help="SSH password", required=True)
    return parser.parse_args()

def load_workers(csv_path):
    """Load the workers in the csv file into the dictionary"""
    with csv_path.open(newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))
    
def process(value):
    """Simple pre-processing of the worker information from the workers.csv file"""
    return (value or "").strip()

def ssh_check(host: str, user: str, password: str) -> tuple[str, str]:
    """Run the SSH check in the specified host"""

    sshpass = shutil.which("sshpass")  # Get the sshpass

    # The ssh command to connect to the specific host using the given password and username
    cmd_parts = [
        shlex.quote(sshpass),
        "-p",
        shlex.quote(password),
        "ssh",
        "-oStrictHostKeyChecking=no",  # Automatically accept it
        "-oUserKnownHostsFile=/dev/null",  # Create a new list of connections
        "-oConnectTimeout=5",  # 5-second timeout for the connection
        f"{shlex.quote(user)}@{shlex.quote(host)}",  # user@136.244.224.___
        "echo ok"]  # If it runs successfully print ok
    
    # Merge and run the command
    cmd = " ".join(cmd_parts)
    result = subprocess.run(cmd, shell=True, text=True, capture_output=True)
    if result.returncode == 0:
        return "ok", ""
    return "failed", f"\n[stderr]\n{result.stderr.strip()}\n\n[stdout]\n{result.stdout.strip()}"  # Return the stderr/stdout as the error message

def ping_workers():
    args = parse_args()

    # If the workers.csv does not exist, raise an error
    if not WORKER_FILE.exists():
        raise(f"CSV not found: {WORKER_FILE}")

    print(f"Scanning workers from {WORKER_FILE}")

    counts = {"ok": 0, "failed": 0}  # Success / Non-Success counts
    messages = []  # Error details from the runs
    
    print(f"{'ROOM':<5} {'HOSTNAME':<20} {'IP-ADDRESS':<15} {'MONITOR':<15} {'SUCCESS'}")

    for worker in load_workers(WORKER_FILE):
        # Get the worker details
        room = process(worker.get("room"))
        hostname = process(worker.get("hostname"))
        host_ip = process(worker.get("ip-address"))
        monitor = process(worker.get("monitor-name"))
        username = process(worker.get("username")) or args.username
        password = process(worker.get("password")) or args.password

        # Ping the ssh in the specific host
        status, details = ssh_check(host_ip, username, password)
        
        # Keep track of how we had been doing
        counts[status] += 1
        success = {"ok": "YES", "failed": "NO"}.get(status)
        
        print(f"{room:<5} {hostname:<20} {host_ip:<15} {monitor:<15} {success}")
        
        # If it errored, add the error message
        if details:
            messages.append(f"{hostname or host_ip}: {details}")

    print(f"\n\n[SUMMARY] {counts['ok']} ok, {counts['failed']} failed, ")
    
    if messages:
        print("\nError Message:")
        for message in messages:
            print(f"--> {message}")

if __name__ == "__main__":
    ping_workers()
