import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from processing.transformations.convert_to_silver import transform_to_silver
from processing.transformations.convert_to_silver import check_silver_parquet

transform_to_silver("2026-06-18")
check_silver_parquet()

