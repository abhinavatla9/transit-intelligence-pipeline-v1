import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from processing.transformations.loadGold_to_db import load_route_metrics_to_db

print("extracting from gold to load into DB")
load_route_metrics_to_db()
print("Successfully Loaded")