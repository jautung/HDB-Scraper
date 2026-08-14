# HDB Scraper (API)

A way to get all the listings from the [HDB resale portal](https://homes.hdb.gov.sg/home/finding-a-flat) by calling its public API directly — no browser automation and no Google Maps API key.

## Usage

### Setup requirements

```bash
python3 -m venv venv
source venv/activate
pip install bs4
pip install requests
pip install googlemaps
```

### Google Maps API billing / usage

If you need to run the MRT augmentation (distance matrix / geocoding) you must set `GOOGLE_MAPS_API_KEY` in your environment. Check your Google Maps usage and billing here:

- Metrics: https://console.cloud.google.com/google/maps-apis/metrics?project=first-server-449508-n0&inv=1&invt=Ab3etw
- Billing account: https://console.cloud.google.com/billing/016D1B-EEA421-736499/reports?project=first-server-449508-n0&inv=1&invt=Ab3etw

### To run

This is intentionally written to be run in two stages,
with checkpointing output `.csv`s along the way,
so that we can save our work and always resume our work
in the event of any unexpected errors (including network errors).

Run both scripts from this directory (`claude/`). All outputs go into
`output/` (git-ignored):

1. **`mrt_precompute.py`**:
   Pre-compute MRT station latitudes/longitudes used for straight-line distance lookups.
   - Output into `output/mrt_lat_lon.csv`
   - Fast: typically < 1 minute

2. **`scrape_listings.py`**:
   Fetch the full listing index from the HDB map API in one call and flatten it to one row per listing.
   - Output into `output/hdb_resale_listings.csv`
   - Fast: typically < 1 minute

3. **`scrape_details.py`**:
   For each `listing_id` from step 1, fetch the full detail record (photos, coordinates, upgrading info, agent details, etc.).
   - Output into `output/hdb_resale_details.csv` (overwritten each run)
   - Long pull: this is the slow step (many listings)

4. **`mrt_augmenter.py`**:
   Fill the MRT-related columns in `output/hdb_resale_details.csv` only:
     - `nearest_mrt_station`, `walking_duration_mins`, `straight_line_distance_km`, `walking_distance_km`
   - Reads `output/mrt_lat_lon.csv` and calls Google Maps for walking distances where available.
   - Long pull: may be slow if you augment many rows (supports `--limit` for testing)

So, running the full sequence will look like:

```bash
cd claude
python3 mrt_precompute.py
python3 scrape_listings.py
python3 scrape_details.py
python3 mrt_augmenter.py
```

Notes on output files and re-runs

- You do not need to delete files in `output/` before re-running: the scripts are designed to resume or overwrite appropriately.
- The quick outputs (`mrt_lat_lon.csv` and `hdb_resale_listings.csv`) are fast to regenerate and safe to re-run frequently.
- The main details file `hdb_resale_details.csv` is the long-lived, canonical dataset of detail rows and is overwritten by `scrape_details.py` each run. `mrt_augmenter.py` updates MRT-related columns in-place (only those four columns).

Step 3 (`scrape_details.py`) is resumable: if the output CSV already exists, listings that succeeded on a previous run are preserved; rerun to retry failures.

For a quick test run of step 2 on just a handful of listings:

```bash
python3 scrape_details.py --limit 10
```

### To debug

```bash
python3 scrape_listings.py --debug 2>&1 | tee output/scrape_listings_out.txt
python3 scrape_details.py --debug 2>&1 | tee output/scrape_details_out.txt
```

### To lint

```bash
black .
```
