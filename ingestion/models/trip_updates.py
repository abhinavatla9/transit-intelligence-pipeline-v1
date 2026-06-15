from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime

@dataclass
class StopTimeUpdate:
    stop_id: str
    arrival_delay: Optional[int] = None
    departure_delay: Optional[int] = None
    arrival_time: Optional[int] = None
    departure_time: Optional[int] = None

@dataclass
class TripUpdate:
    trip_id: str
    route_id: str
    direction_id: str
    vehicle_id: str
    vehicle_label: str
    timestamp: int
    stop_time_updates: list[StopTimeUpdate] = field(default_factory=list)
    ingested_at: str = field(default_factory=lambda: datetime.now().isoformat())