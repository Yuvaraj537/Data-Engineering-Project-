import requests
import pandas as pd
import time

# API URL
url = "https://api.coingecko.com/api/v3/coins/markets"

# Your API Key
API_KEY = "CG-qYfkLdhGSR9m1KV5XuYa9JPL"

# Headers
headers = {
    "accept": "application/json",
    "x-cg-demo-api-key": API_KEY
}

# Store all data
all_data = []

# CoinGecko allows max 250 per page
# 250 × 40 pages = 10,000 records

for page in range(1, 41):

    print(f"Downloading page {page}...")

    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 250,
        "page": page,
        "sparkline": False
    }

    response = requests.get(url, params=params, headers=headers)

    # Convert JSON
    data = response.json()

    # Stop if no data
    if not data:
        print("No more data available")
        break

    # Add to master list
    all_data.extend(data)

    # Avoid rate limit
    time.sleep(2)

# Convert to DataFrame
df = pd.DataFrame(all_data)

# Select important columns
columns_needed = [
    "id",
    "symbol",
    "name",
    "current_price",
    "market_cap",
    "market_cap_rank",
    "total_volume",
    "high_24h",
    "low_24h",
    "price_change_24h",
    "price_change_percentage_24h",
    "circulating_supply",
    "total_supply",
    "max_supply",
    "ath",
    "atl"
]

# Keep only available columns
df = df[[col for col in columns_needed if col in df.columns]]

# Show total records
print("Total Records:", len(df))

# Save CSV
df.to_csv("crypto_10000_records.csv", index=False)

print("CSV saved successfully!")