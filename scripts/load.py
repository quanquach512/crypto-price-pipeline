import os
from datetime import datetime,timezone
import snowflake.connector
from dotenv import load_dotenv
import logging
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = PROJECT_ROOT/"config/.env"

load_dotenv(ENV_FILE)
logging.basicConfig(level=logging.INFO)

#Snowflake config
SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": "CRYPTO_WH",
    "database": "CRYPTO_PIPELINE_DB",
    "schema": "RAW"
}

MERGE_SQL = """
MERGE INTO PRICE_SNAPSHOTS t 
USING ( 
    SELECT %s AS asset_symbol,
           %s AS snapshot_time,
           %s AS price_usd,
           %s AS market_cap_usd,
           %s AS volume_24h_usd,
           %s AS source
) s
ON t.asset_symbol = s.asset_symbol 
AND t.snapshot_time = s.snapshot_time
AND t.source = s.source
WHEN MATCHED THEN 
    UPDATE SET 
        price_usd = s.price_usd,
        market_cap_usd = s.market_cap_usd,
        volume_24h_usd = s.volume_24h_usd
WHEN NOT MATCHED THEN
    INSERT (
        asset_symbol,
        snapshot_time,
        price_usd,
        market_cap_usd,
        volume_24h_usd,
        source
    )
    VALUES (
        s.asset_symbol,
        s.snapshot_time,
        s.price_usd,
        s.market_cap_usd,
        s.volume_24h_usd,
        s.source
    );
"""

def load_prices(rows):
    if not rows:
        logging.info("No rows to load.")
        return 0
    #fixed_snapshot_time = datetime(2026, 4, 4, 17, 30, 0, tzinfo=timezone.utc)

    logging.info("Connecting to Snowflake...")
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cur = conn.cursor()
    try:
        for r in rows:
            logging.info(f"row={r}")
            cur.execute(
                MERGE_SQL, 
                (
                    r["asset_symbol"],
                    r["snapshot_time"],
                    r["price_usd"],
                    r["market_cap_usd"],
                    r["volume_24h_usd"],  
                    r["source"]     
                ), 
            )
        conn.commit()
        logging.info("Merge completed")
        return len(rows)
    finally:
        cur.close()
        conn.close()



