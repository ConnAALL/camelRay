from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

WORKER_FILE = ROOT / "workers.csv"
ENV_FILE = ROOT / ".env"
CONFIG_FILE = ROOT / "config.yml"
TEMP_DIR = ROOT / "temp"
