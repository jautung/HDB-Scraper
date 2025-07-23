# pylint: disable=import-error,missing-module-docstring,missing-function-docstring
import os
import googlemaps

OUTPUT_FOLDER = "output"
PRECOMPUTE_FILENAME = "mrt_lat_lon.csv"


def maybe_create_output_folder():
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)


def get_gmaps_client():
    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    return googlemaps.Client(key=api_key)


# CACHED_LAT_LON = dict()

# def get_lat_lon_from_address_with_cache(gmaps, address):
#     # logger.debug(f"Getting Google Maps geocode of '{address}'")
#     if address in CACHED_LAT_LON:
#         # logger.debug(f"Hit cache for Google Maps geocode of '{address}'!")
#         return CACHED_LAT_LON[address]
#     # logger.debug(f"Missed cache for Google Maps geocode of '{address}'")
#     geocode_result = gmaps.geocode(address=address)
#     if geocode_result is not None:
#         # logger.debug(f"Successfully retrieved Google Maps geocode of '{address}'")
#         location = geocode_result[0]["geometry"]["location"]
#         lat_lon = (location["lat"], location["lng"])
#         CACHED_LAT_LON[address] = lat_lon
#         return lat_lon
#     # logger.error(f"Failed to get Google Maps geocode of '{address}'")
#     return None, None

# def get_lat_lon_from_address(gmaps, address):
#     geocode_result = gmaps.geocode(address=address)
#     if geocode_result is not None:
#         # logger.debug(f"Successfully retrieved Google Maps geocode of '{address}'")
#         location = geocode_result[0]["geometry"]["location"]
#         lat_lon = (location["lat"], location["lng"])
#         return lat_lon
#     # logger.error(f"Failed to get Google Maps geocode of '{address}'")
#     return None, None


def get_lat_lon_from_address(gmaps, address):
    geocode_result = gmaps.geocode(address=address)
    location = geocode_result[0]["geometry"]["location"]
    return (location["lat"], location["lng"])
