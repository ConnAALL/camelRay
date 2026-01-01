#!/usr/bin/env python3

import os
import socket
import select
import threading
import socketserver
import webbrowser
import paramiko
from scripts.ray_setup import get_default_head_ip, get_grid_head_ip
from camelray import get_local_host_and_ips, prompt_creds

RAY_PORT = "8265"
EXPECTED_SUBNET_PREFIX = "136.244.224."


def is_expected_subnet(ips: list[str]) -> bool:
    """Check if the IP is in the expected subnet."""
    return any(ip.startswith(EXPECTED_SUBNET_PREFIX) for ip in (ips or []))


def find_free_local_port(preferred: int) -> int:
    """Return the preferred port if available, else pick an ephemeral free port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("127.0.0.1", int(preferred)))
            return int(preferred)
        except OSError:
            s.bind(("127.0.0.1", 0))
            return int(s.getsockname()[1])


def start_local_port_forward(jump_host: str, username: str, password: str, remote_host: str, remote_port: int, local_port: int):
    """
    Start a local TCP server on 127.0.0.1:<local_port> that forwards connections
    through an SSH session to `remote_host:remote_port` via `jump_host`.
    """

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(jump_host, username=username, password=password, timeout=10, auth_timeout=10, banner_timeout=10)
    transport = ssh.get_transport()
    if transport is None or not transport.is_active():
        raise RuntimeError("SSH transport not active")

    class Handler(socketserver.BaseRequestHandler):
        def handle(self):
            try:
                chan = transport.open_channel("direct-tcpip", (remote_host, int(remote_port)), self.request.getsockname())
            except Exception:
                return

            if chan is None:
                return

            try:
                while True:
                    r, _w, _x = select.select([self.request, chan], [], [])
                    if self.request in r:
                        data = self.request.recv(4096)
                        if not data:
                            break
                        chan.sendall(data)
                    if chan in r:
                        data = chan.recv(4096)
                        if not data:
                            break
                        self.request.sendall(data)
            finally:
                try:
                    chan.close()
                except Exception:
                    pass

    class ForwardServer(socketserver.ThreadingTCPServer):
        daemon_threads = True
        allow_reuse_address = True

    server = ForwardServer(("127.0.0.1", int(local_port)), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, ssh

def main():
    _, ips = get_local_host_and_ips()
    if ips:
        print(f"Your IPs: {', '.join(ips)}")
    else:
        print("Your IPs: unknown")

    head_ip = get_default_head_ip()
    full_address = f"http://{head_ip}:{RAY_PORT}/"  # Direct address of the running ray cluster
    firefox_path = "/usr/bin/firefox-esr"  # Firefox path in the cluster worker nodes

    # If we're not on the expected subnet, tunnel through the Grid Head.
    if not is_expected_subnet(ips):
        grid_head = get_grid_head_ip()
        if not grid_head:
            raise RuntimeError("GRID_HEAD_IP missing from config.yml; cannot setup SSH tunnel")

        creds = prompt_creds()
        local_port = find_free_local_port(int(RAY_PORT))
        server, ssh = start_local_port_forward(grid_head, creds.username, creds.password, head_ip, int(RAY_PORT), local_port)
        tunneled_url = f"http://127.0.0.1:{local_port}/"
        print(f"Not on expected subnet {EXPECTED_SUBNET_PREFIX} → SSH tunnel via Grid Head {grid_head}")
        print(f"Opening: {tunneled_url}")

        if os.path.exists(firefox_path):  # If this is a machine with firefox-esr, specifically use it
            webbrowser.register("myfirefox", None, webbrowser.GenericBrowser(firefox_path))
            webbrowser.get("myfirefox").open_new_tab(tunneled_url)
        else:
            webbrowser.open_new_tab(tunneled_url)

        print("Tunnel active. Press Ctrl-C to close.")
        try:
            while True:
                threading.Event().wait(3600)
        except KeyboardInterrupt:
            pass
        finally:
            try:
                server.shutdown()
                server.server_close()
            except Exception:
                pass
            try:
                ssh.close()
            except Exception:
                pass
        return

    # On expected subnet: open directly.
    if os.path.exists(firefox_path):  # If this is a machine with firefox-esr, specifically use it
        webbrowser.register("myfirefox", None, webbrowser.GenericBrowser(firefox_path))
        webbrowser.get("myfirefox").open_new_tab(full_address)
    else:
        webbrowser.open_new_tab(full_address)


if __name__ == "__main__":
    main()

