from scripts.fetch import fetch_prices
from scripts.load import load_prices

def main():
    rows = fetch_prices()
    load_prices(rows)

if __name__ == "__main__":
    main()