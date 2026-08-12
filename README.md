# HDB Scraper

A way to get all the listings from the [HDB resale portal](https://homes.hdb.gov.sg/home/finding-a-flat).

Currently adding more capabilities to get listings from [PropertyGuru](https://www.propertyguru.com.sg/property-for-sale) as well.

## To check credits

- [Metrics](https://console.cloud.google.com/google/maps-apis/metrics?project=first-server-449508-n0&inv=1&invt=Ab3etw).
- [Billing account](https://console.cloud.google.com/billing/016D1B-EEA421-736499/reports?project=first-server-449508-n0&inv=1&invt=Ab3etw).

## Usage

### Setup requirements (`pip list`)

```
Package            Version
------------------ ---------
appdirs            1.4.4
beautifulsoup4     4.15.0
certifi            2026.7.22
charset-normalizer 3.4.9
googlemaps         4.10.0
idna               3.18
importlib_metadata 9.0.0
pip                26.1.1
pyee               11.1.1
pyppeteer          2.0.0
requests           2.34.2
soupsieve          2.9.2
tqdm               4.70.0
typing_extensions  4.16.0
urllib3            1.26.20
websockets         10.4
zipp               4.1.0
```

### To run (HDB resale portal)

This is intentionally written to be run in multiple stages,
with checkpointing output `.csv`s along the way,
so that we can save our work and always resume our work
in the event of any unexpected errors (including network errors).

1. **`mrt_precompute.py`**:
   Pre-compute the latitudes and longitudes of all MRT stations
   - Output into `output/mrt_lat_lon.csv`
2. **`hdb_listing_pages.py`**:
   Scrape the URLs of all the HDB resale listings
   - Output into `output/listing_urls.csv`
3. **`hdb_base_scraper.py`**:
   From the list of `listing_urls.csv`, scrape _basic_ information about
   these listings, i.e. whatever is shown on the browser
   (notably _excluding_ nearest MRT information)
   - Output into `output/listing_info.csv`
4. **`hdb_scraper.py`**:
   From the basic information of these listings _and_ the pre-computed
   MRT station information, output the final results
   - Output into `output/listings.csv`

So, running the full sequence will look like:

```bash
# Remember to set GOOGLE_MAPS_API_KEY
python3 mrt_precompute.py
python3 hdb_listing_pages.py
python3 hdb_base_scraper.py
python3 hdb_scraper.py
```

### To debug (HDB resale portal)

```bash
python3 mrt_precompute.py --log_level DEBUG 2>&1 | tee output/mrt_precompute_out.txt
python3 hdb_listing_pages.py --log_level DEBUG 2>&1 | tee output/hdb_listing_pages_out.txt
python3 hdb_base_scraper.py --log_level DEBUG 2>&1 | tee output/hdb_base_scraper_out.txt
python3 hdb_scraper.py --log_level DEBUG 2>&1 | tee output/hdb_scraper_out.txt
```

### To run (PropertyGuru)

```bash
python3 pg_listing_pages.py
python3 pg_base_scraper.py
```

### To lint

```bash
black .
```

