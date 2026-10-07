from __future__ import annotations

import os
from pathlib import Path

from ml.features import build_features_from_database

ROOT = Path(__file__).resolve().parents[1]
database = Path(os.getenv("NETWORK_INTELLIGENCE_DB", ROOT / "data" / "warehouse" / "network_intelligence.db"))
output = Path(os.getenv("NETWORK_FEATURES_FILE", ROOT / "data" / "analytics" / "grid_features.parquet"))

if __name__ == "__main__":
    result = build_features_from_database(database, output)
    print(f"Wrote {len(result):,} feature rows to {output}")
