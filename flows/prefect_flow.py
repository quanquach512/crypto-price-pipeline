from prefect import flow
import logging 
from scripts.fetch import fetch_prices
from scripts.load import load_prices


@task
def fetch_task():
    logging.info("[FLOW] Fetching data...")
    rows = fetch_prices()
    logging.info(f"[FLOW] Fetched {len(rows)} rows")
    return rows

@task
def load_task(rows):
    return load_prices(rows)

@flow(name="crypto-price-pipeline")
def crypto_pipeline():
    rows = fetch_task()
    load_task(rows)

if __name__ == "__main__":
    crypto_pipeline()