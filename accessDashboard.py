#!/usr/bin/env python3

import os
import webbrowser
from scripts.ray_setup import get_default_head_ip
from camelray import get_local_host_and_ips

RAY_PORT = "8265"

def main():
    _, ips = get_local_host_and_ips()
    if ips:
        print(f"Your IPs: {', '.join(ips)}")
    else:
        print("Your IPs: unknown")

    head_ip = get_default_head_ip()
    full_address = f"http://{head_ip}:{RAY_PORT}/"  # Direct address of the running ray cluster
    firefox_path = "/usr/bin/firefox-esr"  # Firefox path in the cluster worker nodes

    if os.path.exists(firefox_path):  # If this is a machine with firefox-esr, specifically use it
        # Use firefox-esr explicitly
        webbrowser.register('myfirefox', None, webbrowser.GenericBrowser(firefox_path))
        webbrowser.get('myfirefox').open_new_tab(full_address)
    else:
        # Otherwise use a generic one
        webbrowser.open_new_tab(full_address)


if __name__ == "__main__":
    main()

