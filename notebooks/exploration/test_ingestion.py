import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from ingestion.extractors.bart_call import feed_fetch
from ingestion.loader import load_to_bronze

api_key = "b620e5cd-6345-4bc5-a9cd-b1b8b8dfb068"

feed = feed_fetch("trip_updates", api_key)
print(f"{len(feed.entity)} fetched")

load_to_bronze(feed, "trip_updates")

print("Loaded to bronze bucket")