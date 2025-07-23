# pylint: disable=import-error,missing-module-docstring,missing-class-docstring,missing-function-docstring,too-few-public-methods,too-many-instance-attributes,line-too-long,logging-fstring-interpolation,broad-exception-caught
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
