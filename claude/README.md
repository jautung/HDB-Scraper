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

### To run

This is intentionally written to be run in two stages,
with checkpointing output `.csv`s along the way,
so that we can save our work and always resume our work
in the event of any unexpected errors (including network errors).

Run both scripts from this directory (`claude/`). All outputs go into
`output/` (git-ignored):

1. **`scrape_listings.py`**:
   Fetch the full listing index from the HDB map API in one call,
   then flatten it to one row per listing
   - Output into `output/hdb_resale_listings.csv`
2. **`scrape_details.py`**:
   For each `listing_id` from step 1, fetch the full detail record
   (photos, coordinates, upgrading info, agent details, etc.)
   - Output into `output/hdb_resale_details.csv`

So, running the full sequence will look like:

```bash
cd claude
python3 scrape_listings.py
python3 scrape_details.py
```

Step 2 is resumable: if the output CSVs already exist, listings that
succeeded on a previous run are skipped. Re-run the same command to
retry failures. Use `--overwrite` on `scrape_details.py` to start fresh.

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
