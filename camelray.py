#!/usr/bin/env python3

"""
Interactive terminal menu for managing the Camel Ray cluster.
"""

import subprocess
import sys
from pathlib import Path
import termios
import tty
import socket
import shlex
import platform
import re

from rich.console import Console
from rich.prompt import Prompt
from rich.table import Table

from scripts import ping_workers


ROOT = Path(__file__).resolve().parent
BANNER_FILE = ROOT / "assets" / "banner.txt"

console = Console()
CACHED_CREDS = None
CACHED_CONDA_ENV = None


class Creds:
    def __init__(self, username, password):
        self.username = username
        self.password = password


def read_banner():
    return BANNER_FILE.read_text(encoding="utf-8").rstrip("\n")


def print_banner():
    banner = read_banner()
    console.clear()
    console.print(f"[bold cyan]{banner}[/bold cyan]")
    console.print()

def get_local_host_and_ips():
    """
    Return (hostname, ips) for the current machine.

    `ips` is a best-effort list of IPv4 addresses (loopback excluded when possible).
    """
    try:
        host = socket.gethostname().strip()
    except Exception:
        host = "unknown"

    system = platform.system().lower()
    ips = []

    if system == "linux":
        try:
            out = subprocess.check_output(["hostname", "-I"], text=True).strip()
            ips = [ip for ip in out.split() if ip]
        except Exception:
            ips = []
    elif system == "darwin":
        # macOS doesn't support `hostname -I`. Parse ifconfig output.
        try:
            out = subprocess.check_output(["ifconfig"], text=True, stderr=subprocess.STDOUT)
            # capture inet IPv4 addresses, excluding loopback
            ips = [m.group(1) for m in re.finditer(r"\binet\s+(\d+\.\d+\.\d+\.\d+)\b", out)]
            ips = [ip for ip in ips if ip != "127.0.0.1"]
        except Exception:
            ips = []
    elif system == "windows":
        # Parse ipconfig output for IPv4 addresses.
        try:
            out = subprocess.check_output(["ipconfig"], text=True, stderr=subprocess.STDOUT)
            ips = [m.group(1) for m in re.finditer(r"IPv4 Address[^\d]*(\d+\.\d+\.\d+\.\d+)", out)]
            ips = [ip for ip in ips if ip != "127.0.0.1"]
        except Exception:
            ips = []
    else:
        # Fallback: best-effort from hostname resolution.
        try:
            infos = socket.getaddrinfo(host, None)
            ips = list({info[4][0] for info in infos if info and info[4]})
        except Exception:
            ips = []

    return host, ips

def print_network_info():
    """
    Print hostname + current IPs and warn if we're not on the expected 136.244.224.* subnet.
    """
    host, ips = get_local_host_and_ips()

    console.print(f"[dim]Host:[/dim] {host}")
    console.print(f"[dim]IPs:[/dim]  {', '.join(ips) if ips else 'unknown'}")

    ok_subnet = any(ip.startswith("136.244.224.") for ip in ips)
    if not ok_subnet:
        console.print("[yellow]Warning: not on expected subnet 136.244.224.*[/yellow]")
    console.print()

def print_main_menu_header():
    """Render the main menu header (banner + network info + one-time notices)."""
    print_banner()
    print_network_info()
    init_env_creds()
    init_local_conda_env()

def init_env_creds():
    """Load USERNAME/PASSWORD from .env once (in-memory only) and print a notice."""
    global CACHED_CREDS
    if CACHED_CREDS is not None:
        return
    env_defaults = ping_workers.load_env_defaults()
    default_user = (env_defaults.get("USERNAME") or "").strip()
    default_pass = (env_defaults.get("PASSWORD") or "").strip()
    if default_user and default_pass:
        CACHED_CREDS = Creds(username=default_user, password=default_pass)
        console.print("[yellow]Credentials loaded from .env[/yellow]")

def init_local_conda_env():
    """
    Try to detect the local conda env name from workers.csv for this machine.
    Caches it in-memory only.
    """
    global CACHED_CONDA_ENV
    if CACHED_CONDA_ENV:
        return

    try:
        hostname = socket.gethostname().strip()
    except Exception:
        hostname = ""

    workers = ping_workers.load_workers(ping_workers.WORKER_FILE)
    host_lc = hostname.lower()
    match = next(
        (
            w
            for w in workers
            if host_lc
            and ping_workers.normalize(w.get("hostname")).lower() == host_lc
        ),
        None,
    )
    if match:
        env_name = ping_workers.normalize(match.get("env"))
        if env_name:
            CACHED_CONDA_ENV = env_name
            console.print(f"[yellow]Conda env detected from workers.csv: {env_name}[/yellow]\n")
            return

    # If there is no env specified for this machine, assume no conda activation is needed.
    CACHED_CONDA_ENV = ""

def get_local_conda_env():
    """Return detected local conda env name, or '' if none (no activation needed)."""
    global CACHED_CONDA_ENV
    if CACHED_CONDA_ENV is None:
        init_local_conda_env()
    return CACHED_CONDA_ENV or ""

def prompt_creds():
    if CACHED_CREDS is not None:
        return CACHED_CREDS

    env_defaults = ping_workers.load_env_defaults()
    env_file_exists = ping_workers.ENV_FILE.exists()
    default_user = (env_defaults.get("USERNAME") or "").strip()
    default_pass = (env_defaults.get("PASSWORD") or "").strip()

    have_env_creds = bool(default_user and default_pass)

    # If .env exists and has both credentials, use them automatically without prompting
    if env_file_exists and have_env_creds:
        return Creds(username=default_user, password=default_pass)

    # Otherwise, prompt for credentials
    if not env_file_exists or not have_env_creds:
        console.print("[yellow]No SSH credentials found in `.env`.[/yellow]")
        console.print("You can either enter them now (they will NOT be saved), or add them to `.env` yourself:")
        console.print(f"[dim]{ping_workers.ENV_FILE}[/dim]")
        console.print("[dim]USERNAME=your_username[/dim]")
        console.print("[dim]PASSWORD=your_password[/dim]\n")

        # Require explicit input when we don't have valid .env defaults.
        username = ""
        while not username:
            username = Prompt.ask("SSH username").strip()
        password = ""
        while not password:
            password = Prompt.ask("SSH password", password=True).strip()
        return Creds(username=username, password=password)


def run_script(script_name, args):
    """
    Run one of the repo's scripts as a child process, streaming output to the terminal.
    """
    stem = Path(script_name).stem
    module = f"scripts.{stem}"

    env_name = get_local_conda_env()
    if env_name:
        conda_sh = '$HOME/miniconda3/etc/profile.d/conda.sh'
        qenv = shlex.quote(env_name)
        qargs = " ".join(shlex.quote(a) for a in args)

        # Run inside conda env so Ray/Python versions match the cluster.
        bash_cmd = f'source {conda_sh} && conda activate {qenv} && python3 -m {shlex.quote(module)} {qargs}'.strip()
        return subprocess.call(["bash", "-lc", bash_cmd], cwd=str(ROOT))

    # No env specified for this machine: run with current interpreter.
    cmd = [sys.executable, "-m", module, *args]
    return subprocess.call(cmd, cwd=str(ROOT))

def run_local_pyfile(pyfile: Path, args):
    """
    Run a repo-local .py file as a child process, streaming output to the terminal.
    Uses the detected local conda env when available (to match Ray/Python versions).
    """
    pyfile = Path(pyfile)
    env_name = get_local_conda_env()
    if env_name:
        conda_sh = '$HOME/miniconda3/etc/profile.d/conda.sh'
        qenv = shlex.quote(env_name)
        qfile = shlex.quote(str(pyfile))
        qargs = " ".join(shlex.quote(a) for a in args)
        bash_cmd = f"source {conda_sh} && conda activate {qenv} && python3 {qfile} {qargs}".strip()
        return subprocess.call(["bash", "-lc", bash_cmd], cwd=str(ROOT))

    cmd = [sys.executable, str(pyfile), *args]
    return subprocess.call(cmd, cwd=str(ROOT))


def menu_table(title, rows):
    table = Table(title=title or None, show_lines=False, box=None)
    table.add_column("#", style="cyan", no_wrap=True, justify="right")
    table.add_column("Action", style="white")
    for k, label in rows:
        table.add_row(k, label)
    console.print(table)

def read_menu_choice(choices, prompt="Select"):
    """
    Read a single keypress (no Enter) and return it if it is in `choices`.
    Linux/Unix only (uses termios/tty).
    """
    # Normalize to lower-case so we can treat 'B' the same as 'b', etc.
    choices_set = {c.lower() for c in choices}
    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    try:
        while True:
            console.print(f"[bold]{prompt}[/bold] ({'/'.join(choices)}): ", end="")
            sys.stdout.flush()
            # cbreak keeps signal handling (Ctrl-C) working, unlike raw mode.
            tty.setcbreak(fd)
            ch = sys.stdin.read(1).lower()
            # restore cooked mode so echo/newlines behave normally for printing
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
            console.print(ch)  # echo selection + newline
            if ch in choices_set:
                return ch
            console.print("[yellow]Invalid selection.[/yellow]\n")
    finally:
        try:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        except Exception:
            pass


def action_check_workers():
    creds = prompt_creds()
    run_script("ping_workers", ["--username", creds.username, "--password", creds.password])

def action_access_dashboard():
    # accessDashboard.py handles its own credential prompting + optional SSH tunneling.
    run_local_pyfile(ROOT / "accessDashboard.py", [])


def action_stop_cluster():
    creds = prompt_creds()
    # ray_stop supports --workers selection; we keep it optional here.
    selectors = Prompt.ask("Optional worker selectors (comma-separated) [leave empty for all]", default="").strip()
    args = ["--username", creds.username, "--password", creds.password]
    if selectors:
        args += ["--workers", selectors]
    run_script("ray_stop", args)

def action_restart_cluster():
    """
    Restart the Ray cluster by stopping everything and then starting from scratch.
    """
    console.print("[yellow]Restart will STOP the existing cluster and then start a fresh one.[/yellow]\n")
    creds = prompt_creds()

    stop_rc = run_script("ray_stop", ["--username", creds.username, "--password", creds.password])
    if stop_rc != 0:
        console.print(f"[yellow]Warning: ray_stop exited with code {stop_rc}. Continuing with restart...[/yellow]\n")

    start_rc = run_script("ray_setup", ["--username", creds.username, "--password", creds.password])
    if start_rc != 0:
        console.print(f"[red]ray_setup exited with code {start_rc}[/red]\n")


def manage_cluster_menu():
    while True:
        print_banner()
        menu_table(
            None,
            [
                ("1", "View Ray Workers"),
                ("2", "Start cluster"),
                ("3", "Stop Cluster"),
                ("4", "Restart Cluster"),
                ("b", "Back"),
            ],
        )
        choice = read_menu_choice(["1", "2", "3", "4", "b"])
        if choice == "1":
            run_script("ray_diagnosis", [])
            Prompt.ask("\nPress Enter to continue", default="")
            print_banner()
        elif choice == "2":
            start_cluster_menu()
        elif choice == "3":
            action_stop_cluster()
            Prompt.ask("\nPress Enter to continue", default="")
            print_banner()
        elif choice == "4":
            action_restart_cluster()
            Prompt.ask("\nPress Enter to continue", default="")
            print_banner()
        else:
            return


def start_cluster_menu():
    while True:
        print_banner()
        menu_table(
            None,
            [
                ("1", "From scratch (will STOP existing cluster first)"),
                ("2", "Prune / reconnect (only start Ray where it's missing)"),
                ("b", "Back"),
            ],
        )
        choice = read_menu_choice(["1", "2", "b"])
        if choice == "1":
            console.print("[yellow]From scratch will stop any existing Ray processes and start a fresh cluster.[/yellow]\n")
            creds = prompt_creds()
            run_script("ray_setup", ["--username", creds.username, "--password", creds.password])
            Prompt.ask("\nPress Enter to continue", default="")
            print_banner()
        elif choice == "2":
            console.print("[yellow]Prune mode will NOT stop existing Ray processes; it only starts Ray where it's not running.[/yellow]\n")
            creds = prompt_creds()
            run_script("ray_setup", ["--username", creds.username, "--password", creds.password, "--prune"])
            Prompt.ask("\nPress Enter to continue", default="")
            print_banner()
        else:
            return


def main():
    try:
        print_main_menu_header()

        while True:
            menu_table(
                None,
                [
                    ("1", "Ping workers"),
                    ("2", "Manage Cluster"),
                    ("3", "Access Dashboard"),
                    ("e", "Exit"),
                ],
            )

            choice = read_menu_choice(["1", "2", "3", "e"])
            pause_after = True
            if choice == "1":
                action_check_workers()
            elif choice == "2":
                manage_cluster_menu()
                pause_after = False  # submenu handles its own pauses; Back should return immediately
            elif choice == "3":
                action_access_dashboard()
            elif choice == "e":
                return

            if pause_after:
                Prompt.ask("\nPress Enter to return to menu", default="")
            print_main_menu_header()
    except KeyboardInterrupt:
        # Exit immediately back to the shell (Ctrl-C).
        console.print("\n[dim]Exiting...[/dim]")
        return

if __name__ == "__main__":
    main()
