# HDB Scraper

A way to get all the listings from the [HDB resale portal](https://homes.hdb.gov.sg/home/finding-a-flat).

## To check credits

- [Google console](https://console.cloud.google.com/google/maps-apis/metrics?project=first-server-449508-n0).
- [Metrics](https://console.cloud.google.com/google/maps-apis/metrics?project=first-server-449508-n0&inv=1&invt=Ab3etw).
- [Billing account](https://console.cloud.google.com/billing/016D1B-EEA421-736499/reports?project=first-server-449508-n0&inv=1&invt=Ab3etw).

## Usage

### To lint

```bash
black hdb_scraper.py
```

### To run

```bash
python3 hdb_scraper.py 2>&1 | tee output.txt
```

### To run one page

```bash
python3 hdb_scraper.py --max_number_of_pages 1 2>&1 | tee output.txt
```

### To debug

```bash
python3 hdb_scraper.py --log_level DEBUG 2>&1 | tee output.txt
```

