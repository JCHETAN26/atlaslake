"""AtlasLake core. The whole platform runs in UTC: date bucketing of timestamptz values (DuckDB,
pandas, Spark collect) must never depend on the host's local timezone."""

import os
import time

os.environ["TZ"] = "UTC"
if hasattr(time, "tzset"):
    time.tzset()
