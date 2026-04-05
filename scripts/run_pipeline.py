from scripts.fetch_coingecko import fetch_coingecko_prices
from scripts.load import load_prices
import logging 

def main():
    rows = fetch_coingecko_prices()
    logging.info(f"[FLOW] From [coingecko] Fetched {len(rows)} rows")
    load_prices(rows)

if __name__ == "__main__":
    main()