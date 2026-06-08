import requests
from google.transit import gtfs_realtime_pb2

api_key = "b620e5cd-6345-4bc5-a9cd-b1b8b8dfb068"
url = f"http://api.511.org/transit/TripUpdates?api_key={api_key}&agency=BA"

response = requests.get(url, timeout=10)

# print(response.content)
feed = gtfs_realtime_pb2.FeedMessage()
feed.ParseFromString(response.content)
print(len(feed.entity))
# print(f"Entities received: {len(feed.entity)}")
# print(f"First entity: {feed.entity[0]}")