import requests
from google.transit import gtfs_realtime_pb2
import logging

logger = logging.getLogger(__name__)

BART_feeds = { "trip_updates": "http://api.511.org/transit/TripUpdates?api_key={api_key}&agency=BA",
              "vehicle_positions": "http://api.511.org/transit/VehiclePositions?api_key={api_key}&agency=BA",
              "service_alerts": "http://api.511.org/transit/ServiceAlerts?api_key={api_key}&agency=BA"}

def feed_fetch(feed_type: str, api_key: str) -> gtfs_realtime_pb2.FeedMessage:
    url = BART_feeds[feed_type].format(api_key=api_key)
    logger.info(f"fetching the url: {url.format(xxx=api_key)}")
    response = requests.get(url, timeout=10)

    response.raise_for_status()

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    logger.info(f"Received {len(feed.entity)} entities")
    return feed

feed_fetch("trip_updates", "b620e5cd-6345-4bc5-a9cd-b1b8b8dfb068")