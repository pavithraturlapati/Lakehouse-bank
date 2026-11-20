"""All file paths in one place, so every script agrees on where data lives."""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"

LANDING_DIR = DATA / "landing" / "customers"          # raw CDC files arrive here
STATE_FILE = DATA / "state" / "generator_state.json"  # generator remembers customers

BRONZE_PATH = DATA / "lake" / "bronze" / "customers_cdc"
QUARANTINE_PATH = DATA / "lake" / "bronze" / "customers_quarantine"
BRONZE_CHECKPOINT = DATA / "checkpoints" / "bronze_customers"

SILVER_PATH = DATA / "lake" / "silver" / "customers"
QUALITY_PATH = DATA / "lake" / "quality" / "results"
