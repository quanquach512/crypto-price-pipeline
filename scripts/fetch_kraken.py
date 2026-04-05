import requests
from datetime import datetime, timezone
import logging 

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")

KRAKEN_URL = "https://api.kraken.com/0/public/Ticker"
KRAKEN_PARAMS = {
    "pair": "XXBTZUSD,XETHZUSD,SOLUSD"
}


SYMBOL_MAP = {
    "XXBTZUSD": "BTC",
    "XBTUSD": "BTC",
    "XETHZUSD": "ETH",
    "ETHUSD": "ETH",
    "SOLUSD": "SOL",
}


def fetch_kraken_prices(snapshot_time=None):
    if snapshot_time is None:
        snapshot_time = datetime.now(timezone.utc).replace(microsecond=0)
    
    response = requests.get(KRAKEN_URL, params=KRAKEN_PARAMS, timeout=30)
    response.raise_for_status()
    data = response.json()
    if data.get("error"):
        raise RuntimeError(f"Kraken API error: {data['error']}")

    result = data["result"]
    rows = []

    for pair_name, pair_data in result.items():
        base = SYMBOL_MAP.get(pair_name)
        if base is None:
            continue
        price_usd = float(pair_data["c"][0])
        volume_24h_usd = None
        if "v" in pair_data and len(pair_data["v"]) > 1:
            try:
                volume_24h_usd = float(pair_data["v"][1]) * price_usd
            except Exception:
                volume_24h_usd = None
        rows.append(
            {
                "asset_symbol": base,
                "snapshot_time": snapshot_time,
                "price_usd": price_usd,
                "market_cap_usd": None,
                "volume_24h_usd": volume_24h_usd,
                "source": "kraken",
            }
        )
    return rows


if __name__ == "__main__":
    rows = fetch_kraken_prices()
    for r in rows:
        print(r)



