import requests
from datetime import datetime, timezone

URL = "https://api.coingecko.com/api/v3/coins/markets"

PARAMS = {
    "vs_currency": "usd",
    "ids": "bitcoin,ethereum,solana"
}

def fetch_coingecko_prices(snapshot_time=None):
    if snapshot_time is None:
        snapshot_time = datetime.now(timezone.utc).replace(microsecond=0)
    response = requests.get(URL, params=PARAMS, timeout=30)
    response.raise_for_status()
    data = response.json()

    res = []
    for coin in data:
        res.append({
            "asset_symbol": coin["symbol"].upper(),
            "snapshot_time": snapshot_time,
            "price_usd": coin["current_price"],
            "market_cap_usd": coin["market_cap"],
            "volume_24h_usd": coin["total_volume"],
            "source":"coingecko"
        })
    return res

if __name__ == "__main__":
    rows = fetch_coingecko_prices()
    for r in rows:
        print(r)