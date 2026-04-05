from prefect import flow, task
import logging 
from scripts.fetch_coingecko import fetch_coingecko_prices
from scripts.load import load_prices

@task
def fetch_task():
    logging.info("[FLOW] Fetching data...")
    rows = fetch_coingecko_prices()
    # logging.info(f"[FLOW] From [coingecko] Fetched {len(rows)} rows")
    return rows

@task
def load_task(rows):
    return load_prices(rows)

@flow(name="crypto-price-pipeline")
def crypto_pipeline():
    rows = fetch_task()
    load_task(rows)

if __name__ == "__main__":
    crypto_pipeline.serve(
        name="crypto-price_deployment",
        cron="*/5 * * * *",
        pause_on_shutdown=False
    )