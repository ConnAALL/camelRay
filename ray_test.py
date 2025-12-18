"""Simple script to print the IP address and the hostname of each worker in the cluster"""

import socket
import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

@ray.remote
def hello():
    return f"Hello from {ray.util.get_node_ip_address()}\t{socket.gethostname()}"

def main():
    ray.init(address="auto")
    
    nodes = [n for n in ray.nodes() if n.get("Alive")]
    node_ids = [n["NodeID"] for n in nodes]

    refs = [hello.options(scheduling_strategy=NodeAffinitySchedulingStrategy(node_id=nid, soft=False)).remote() for nid in node_ids]

    for msg in ray.get(refs):
        print(msg)

if __name__ == "__main__":
    main()
