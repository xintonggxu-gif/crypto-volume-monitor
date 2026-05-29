
import requests
import pandas as pd
from datetime import datetime, timezone


def fetchbinance_quote_bid_ask():
    url = 'https://api.binance.com/api/v3/ticker/24hr'
    resp = requests.get(url, timeout = 20)
    resp.raise_for_status()
    data = resp.json()
    df = pd.DataFrame(data)
    col1 = 'quoteVolume'
    col2 = 'bidPrice'
    col3 = 'askPrice'
    df[col1] = pd.to_numeric(df[col1], errors="coerce")
    df[col2] = pd.to_numeric(df[col2], errors="coerce")
    df[col3] = pd.to_numeric(df[col3], errors="coerce")
    dfvol = df[['symbol', col1, col2, col3]]
    
    return dfvol


def fetchquoteasset():
    url = 'https://api.binance.com/api/v3/exchangeInfo'
    params = {
        "permissions": "SPOT",
        "symbolStatus": "TRADING"
    }

    resp = requests.get(url, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    df = pd.DataFrame(data["symbols"])

    dfasset = df[[
        'symbol',
        'quoteAsset',
        'isSpotTradingAllowed',
        'isMarginTradingAllowed',
        'status'
    ]]

    return dfasset     

def merge_volumebidaskquote(dfvol, dfasset):
    df_all = dfasset.merge(
        dfvol,
        on="symbol",
        how="left"
        )


    return df_all

def pricemap():
    url = 'https://api.binance.com/api/v3/ticker/24hr'
    resp = requests.get(url, timeout = 20)
    resp.raise_for_status()
    data = resp.json()
    df = pd.DataFrame(data)
    col = "weightedAvgPrice"
    df[col] = pd.to_numeric(df[col], errors="coerce")
    price_map = dict(zip(df["symbol"], df["weightedAvgPrice"]))

    return price_map

def coinbase_usdt_rates():
    """
    Coinbase 官方 exchange-rates API.

    返回的 rates 含义是：
    rates["JPY"] = 1 USDT 等于多少 JPY
    rates["USD"] = 1 USDT 等于多少 USD
    """
    url = "https://api.coinbase.com/v2/exchange-rates"
    params = {
        "currency": "USDT"
    }

    resp = requests.get(url, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    rates = data["data"]["rates"]

    # 转成数字
    clean_rates = {}
    for asset, rate in rates.items():
        try:
            clean_rates[asset.upper()] = float(rate)
        except (TypeError, ValueError):
            pass

    return clean_rates        

def get_rate_to_usdt(asset, price_map, coinbase_rates):
    if pd.isna(asset):
        return None

    asset = str(asset).upper().strip()

    if asset == "USDT":
        return 1.0

    # 1. 先用 Binance direct
    # 例如 BTCUSDT = 1 BTC 多少 USDT
    direct_symbol = asset + "USDT"

    if direct_symbol in price_map:
        price = price_map[direct_symbol]

        if pd.notna(price) and price != 0:
            return float(price)

    # 2. 再用 Binance inverse
    # 例如 USDTTRY = 1 USDT 多少 TRY
    # 所以 1 TRY = 1 / USDTTRY USDT
    inverse_symbol = "USDT" + asset

    if inverse_symbol in price_map:
        inverse_price = price_map[inverse_symbol]

        if pd.notna(inverse_price) and inverse_price != 0:
            return 1 / float(inverse_price)

    # 3. Binance 找不到，再用 Coinbase fallback
    # Coinbase rates[asset] = 1 USDT 多少 asset
    # 所以 1 asset = 1 / rates[asset] USDT
    if asset in coinbase_rates:
        usdt_to_asset = coinbase_rates[asset]

        if usdt_to_asset != 0:
            return 1 / usdt_to_asset

    return None

def convert_to_usdt(df_all):
    try:
        price_map = pricemap()
    except Exception as e:
        price_map = {}
        print(f"Warning: failed to load binance fallback rates: {e}")
    
    try:
        coinbase_rates = coinbase_usdt_rates()
    except Exception as e:
        coinbase_rates = {}
        print(f"Warning: failed to load Coinbase fallback rates: {e}")

    df_all = df_all.copy()

    df_all["rate_to_usdt"] = df_all["quoteAsset"].apply(
        lambda asset: get_rate_to_usdt(asset, price_map, coinbase_rates)
    )

    df_all["volume_usdt"] = (
        df_all["quoteVolume"] * df_all["rate_to_usdt"]
    )
    df_all["bidprice_usdt"] = (
        df_all["bidPrice"] * df_all["rate_to_usdt"]
    )
    df_all["askprice_usdt"] = (
        df_all["askPrice"] * df_all["rate_to_usdt"]
    )
    return df_all

def add_run_time(df_selected): 
    df_selected  = df_selected.copy()
    run_date_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    df_selected ["run_date_utc"] = run_date_utc
    return df_selected    
    
def main():
    dfvol = fetchbinance_quote_bid_ask()

    dfasset = fetchquoteasset()

    df_all = merge_volumebidaskquote(dfvol, dfasset)

    df_final = convert_to_usdt(df_all)

    # 加 run date
    df_final = add_run_time(df_final)

    print(df_final[[
    "symbol",
    "quoteVolume",
    "quoteAsset",
    "isMarginTradingAllowed",
    "rate_to_usdt",
    "volume_usdt",
    "bidPrice",
    "askPrice",
    "bidprice_usdt",
    "askprice_usdt",
    "run_date_utc"
    ]].head())
    missing_ticker = df_final[df_final["quoteVolume"].isna()]

    print("Missing ticker data:")
    print(missing_ticker[["symbol", "quoteAsset", "isMarginTradingAllowed"]])
    failed = df_final[
    (df_final["rate_to_usdt"].isna()) &
    (df_final["quoteVolume"] > 0)
    ]

    print("Failed to convert:")
    print(failed[["symbol", "quoteAsset", "quoteVolume"]])

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    output_file = f"binance_volume_usdt_{timestamp}.csv"

    df_final.to_csv(output_file, index=False)

    print(f"Saved to {output_file}")


if __name__ == "__main__":
    main()   
    
    
    
    
