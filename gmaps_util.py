# pylint: disable=import-error,missing-module-docstring,missing-class-docstring,missing-function-docstring,too-few-public-methods,too-many-instance-attributes,too-many-arguments,line-too-long,logging-fstring-interpolation,broad-exception-caught
import math
import os
import googlemaps


def get_gmaps_client():
    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    return googlemaps.Client(key=api_key)


def get_lat_lon_from_address(gmaps, address):
    geocode_result = gmaps.geocode(address=address)
    assert geocode_result is not None
    location = geocode_result[0]["geometry"]["location"]
    return (location["lat"], location["lng"])


def get_walking_distance_and_duration(gmaps, start, end):
    gmaps_result = gmaps.distance_matrix(
        origins=[_adapt_location_name_for_distance_matrix(location_name=start)],
        destinations=[_adapt_location_name_for_distance_matrix(location_name=end)],
        mode="walking",
    )
    gmaps_result_inner = gmaps_result["rows"][0]["elements"][0]
    if gmaps_result_inner["status"] == "OK":
        distance_metres = gmaps_result_inner["distance"]["value"]
        duration_seconds = gmaps_result_inner["duration"]["value"]
        return (distance_metres, duration_seconds)
    assert gmaps_result_inner["status"] == "NOT_FOUND"
    return (None, None)


def _adapt_location_name_for_distance_matrix(location_name):
    if location_name == "HarbourFront MRT station":
        # Don't ask me why, but Google Maps chokes on the name
        # "HarbourFront MRT station", and is unable to find a location for that...
        # "HarbourFront MRT Station (CC29)" is apparently fine though...
        return "HarbourFront MRT Station (CC29)"
    return location_name


# Approximation, based on great-circle distance
def haversine_distance_km(lat1, lon1, lat2, lon2):
    earth_radius = 6371.0  # Radius of the Earth in kilometers
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    )
    return earth_radius * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
