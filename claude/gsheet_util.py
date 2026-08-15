"""Utilities for reading/writing Google Sheets using a service account.

Requires `gspread` and `google-auth` packages.

Functions:
- authorize(creds_path)
- read_sheet_rows(sheet_id, creds_path) -> (fieldnames, rows_list)
- write_sheet_overwrite(sheet_id, creds_path, fieldnames, rows)
- upsert_sheet_by_listing_id(sheet_id, creds_path, fieldnames, rows)
- update_mrt_columns(sheet_id, creds_path, listing_id_to_mrt_values, mrt_fields)
"""

from typing import List, Dict, Tuple
import logging
import string

try:
    import gspread
    from google.oauth2.service_account import Credentials
except Exception:  # pragma: no cover - optional dependency
    gspread = None

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Default Google Sheet ID (shared sheet for HDB details)
DEFAULT_SHEET_ID = "1euqvyslpzbfkJniEbeM1YfrutGtfPCRV4CmroprCb5I"


def _ensure_gspread():
    if gspread is None:
        raise RuntimeError("gspread/google-auth are required for Google Sheets support")


def authorize(creds_path: str):
    _ensure_gspread()
    creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    return gspread.authorize(creds)


def _colnum_to_letter(n: int) -> str:
    # 1-based
    letters = []
    while n > 0:
        n, rem = divmod(n - 1, 26)
        letters.append(string.ascii_uppercase[rem])
    return "".join(reversed(letters))


def read_sheet_rows(
    sheet_id: str, creds_path: str
) -> Tuple[List[str], List[Dict[str, str]]]:
    """Return (fieldnames, rows_list) where rows_list is list of dicts keyed by header."""
    gc = authorize(creds_path)
    sh = gc.open_by_key(sheet_id)
    ws = sh.sheet1
    values = ws.get_all_values()
    if not values:
        return [], []
    headers = values[0]
    rows = []
    for r in values[1:]:
        d = {h: (r[i] if i < len(r) else "") for i, h in enumerate(headers)}
        rows.append(d)
    return headers, rows


def write_sheet_overwrite(
    sheet_id: str, creds_path: str, fieldnames: List[str], rows: List[Dict[str, str]]
):
    gc = authorize(creds_path)
    sh = gc.open_by_key(sheet_id)
    ws = sh.sheet1
    # Prepare values: header + rows
    values = [fieldnames]
    for r in rows:
        values.append([r.get(fn, "") for fn in fieldnames])
    ws.clear()
    ws.append_rows(values, value_input_option="USER_ENTERED")


def upsert_sheet_by_listing_id(
    sheet_id: str, creds_path: str, fieldnames: List[str], rows: List[Dict[str, str]]
):
    """Upsert rows by `listing_id`. Existing rows are replaced, new rows appended.

    This reads the sheet once to build a listing_id->row_index map, then performs
    a batch update for existing rows and a batch append for new rows.
    """
    gc = authorize(creds_path)
    sh = gc.open_by_key(sheet_id)
    ws = sh.sheet1
    existing_headers = ws.row_values(1)
    if not existing_headers:
        # empty sheet: just write all
        write_sheet_overwrite(sheet_id, creds_path, fieldnames, rows)
        return

    # map header -> col index
    header_to_idx = {h: i + 1 for i, h in enumerate(existing_headers)}
    # ensure fieldnames are subset of sheet headers; if not, we will overwrite entire sheet
    if set(fieldnames) - set(existing_headers):
        # incompatible headers: overwrite sheet
        write_sheet_overwrite(sheet_id, creds_path, fieldnames, rows)
        return

    # build existing listing_id -> row index
    values = ws.get_all_values()
    listing_idx = {}
    for i, row in enumerate(values[1:], start=2):
        lid = (
            row[existing_headers.index("listing_id")]
            if "listing_id" in existing_headers
            and len(row) > existing_headers.index("listing_id")
            else ""
        ).strip()
        if lid:
            listing_idx[lid] = i

    # Prepare batch updates for existing rows
    to_update = []
    to_append = []
    for r in rows:
        lid = (r.get("listing_id") or "").strip()
        if lid in listing_idx:
            rownum = listing_idx[lid]
            # build row values in existing header order
            vals = [r.get(h, "") for h in existing_headers]
            # range A{rownum}:<lastcol>{rownum}
            lastcol = _colnum_to_letter(len(existing_headers))
            to_update.append(
                {
                    "range": f"{_colnum_to_letter(1)}{rownum}:{lastcol}{rownum}",
                    "values": [vals],
                }
            )
        else:
            to_append.append([r.get(h, "") for h in existing_headers])

    if to_update:
        # gspread expects a list of {"range":..., "values":[...]} items
        ws.batch_update(to_update, value_input_option="USER_ENTERED")
    if to_append:
        # append only the new rows (do not re-append headers)
        ws.append_rows(to_append, value_input_option="USER_ENTERED")


def update_mrt_columns(
    sheet_id: str,
    creds_path: str,
    listing_id_to_mrt_values: Dict[str, Dict[str, str]],
    mrt_fields: List[str],
):
    """Update only MRT column values for listing_ids that exist in the sheet.

    `listing_id_to_mrt_values` maps listing_id -> {field: value}
    """
    gc = authorize(creds_path)
    sh = gc.open_by_key(sheet_id)
    ws = sh.sheet1
    values = ws.get_all_values()
    if not values or len(values) < 2:
        return 0
    headers = values[0]
    if "listing_id" not in headers:
        return 0
    col_index = {h: i + 1 for i, h in enumerate(headers)}
    lastcol = _colnum_to_letter(len(headers))

    # build listing_id -> rownum map
    listing_idx = {}
    for i, row in enumerate(values[1:], start=2):
        lid = (
            row[headers.index("listing_id")]
            if len(row) > headers.index("listing_id")
            else ""
        )
        if lid:
            listing_idx[lid] = i

    updates = []
    updated_count = 0
    for lid, mvals in listing_id_to_mrt_values.items():
        if lid not in listing_idx:
            continue
        rownum = listing_idx[lid]
        # build full-row values for columns we will update (others empty placeholders)
        vals = [""] * len(headers)
        for f, v in mvals.items():
            if f in col_index:
                vals[col_index[f] - 1] = v
        updates.append({"range": f"A{rownum}:{lastcol}{rownum}", "values": [vals]})
        updated_count += 1

    if updates:
        ws.batch_update(updates, value_input_option="USER_ENTERED")
    return updated_count
