from dataclasses import dataclass


@dataclass
class Location:
    location_id: str
    location_name: str
    latitude: float
    longitude: float
