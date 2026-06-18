from pprint import pprint
trips = [
    {
        "trip_id": "1841975",
        "route_id": "Blue-N",
        "vehicle_id": "3-door",
        "stops": [
            {"stop_id": "905201", "arrival_delay": 714},
            {"stop_id": "905202", "arrival_delay": 600},
            {"stop_id": "905203", "arrival_delay": 550}
        ]
    }
]

records = []
for trip in trips:
    for stop in trip["stops"]:
        records.append({
            "trip_id": trip["trip_id"],
            "route": trip["route"],
            "stop": stop
        })

pprint(records)