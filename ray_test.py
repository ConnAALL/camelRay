"""Simple script to print the IP address and the hostname of each worker in the cluster"""

import socket
from pathlib import Path
import yaml
import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

CONFIG_FILE = Path(__file__).with_name("config.yml")  # Load the configuration file

def load_config():
    """Load the configuration file"""
    with CONFIG_FILE.open(encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

@ray.remote
def hello():
    """Print the IP address and the hostname of the node"""
    return f"Hello from {ray.util.get_node_ip_address()}\t{socket.gethostname()}"

def main():
    config = load_config()
    head_ip = config["DEFAULT_HEAD_IP"]
    ray.init(address=f"{head_ip}:6379")
    
    nodes = [n for n in ray.nodes() if n.get("Alive")]  # Get the list of nodes that are alive
    node_ids = [n["NodeID"] for n in nodes]

    refs = [hello.options(scheduling_strategy=NodeAffinitySchedulingStrategy(node_id=nid, soft=False)).remote() for nid in node_ids]  # Schedule the hello function to the nodes

    for msg in ray.get(refs):  # Get the results of the hello function
        print(msg)

if __name__ == "__main__":
    main()
