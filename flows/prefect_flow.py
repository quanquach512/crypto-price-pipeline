from prefect import flow, task
import logging 
from scripts.fetch_coingecko import fetch_coingecko_prices
from scripts.fetch_kraken import fetch_kraken_prices

from scripts.load import load_prices

@task
def fetch_coingecko_task():
    logging.info("[FLOW] Fetching data from coingecko...")
    rows = fetch_coingecko_prices()
    logging.info(f"[FLOW] From [coingecko] Fetched {len(rows)} rows")
    return rows

@task 
def fetch_kraken_task():
    logging.info("[FLOW] Fetching data from kraken...")
    rows = fetch_kraken_prices()
    logging.info(f"[FLOW] From [kraken] Fetched {len(rows)} rows")
    return rows

@task
def load_task(rows):
    return load_prices(rows)

@flow(name="crypto-price-pipeline")
def crypto_pipeline():
    cgk_rows = fetch_coingecko_task()
    kr_rows = fetch_kraken_task()
    rows = cgk_rows+kr_rows
    load_task(rows)

if __name__ == "__main__":
    crypto_pipeline.serve(
        name="crypto-price_deployment",
        cron="*/5 * * * *",
        pause_on_shutdown=False
    )