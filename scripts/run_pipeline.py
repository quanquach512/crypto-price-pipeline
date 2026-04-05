from scripts.fetch_coingecko import fetch_coingecko_prices
from scripts.fetch_kraken import fetch_kraken_prices
from datetime import datetime, timezone

from scripts.load import load_prices
import logging 

def main():
    snapshot_time = datetime.now(timezone.utc).replace(microsecond=0)
    cgk_rows = fetch_coingecko_prices(snapshot_time=snapshot_time)
    kr_rows = fetch_kraken_prices(snapshot_time=snapshot_time)

    rows = cgk_rows + kr_rows
    # logging.info(f"[FLOW] From [coingecko] Fetched {len(rows)} rows")
    load_prices(rows)

if __name__ == "__main__":
    main()