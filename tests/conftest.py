"""Mock PySpark for unit tests that don't need a real Spark session."""
import sys
from unittest.mock import MagicMock

for mod in [
    "pyspark",
    "pyspark.sql",
    "pyspark.sql.functions",
    "pyspark.sql.types",
    "findspark",
]:
    if mod not in sys.modules:
        sys.modules[mod] = MagicMock()
