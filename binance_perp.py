import pandas as pd
from datetime import datetime, timezone
import httpx
import asyncio

async def fetchbinance_perp_quote_volume():
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.get(url, timeout=20)
        resp.raise_for_status()
        data = resp.json()

    df = pd.DataFrame(data)

    numeric_cols = ["quoteVolume", "volume", "weightedAvgPrice"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    dfvol = df[[
        "symbol",
        "quoteVolume",
        "volume",
        "weightedAvgPrice"
    ]]

    return dfvol


async def fetchbinance_perp_bid_ask():
    url = "https://fapi.binance.com/fapi/v1/ticker/bookTicker"
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.get(url, timeout=20)
        resp.raise_for_status()
        data = resp.json()

    df = pd.DataFrame(data)

    df["bidPrice"] = pd.to_numeric(df["bidPrice"], errors="coerce")
    df["askPrice"] = pd.to_numeric(df["askPrice"], errors="coerce")

    df_bidask = df[[
        "symbol",
        "bidPrice",
        "askPrice"
    ]]

    return df_bidask


async def fetchperp_exchangeinfo():
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.get(url, timeout=20)
        resp.raise_for_status()
        data = resp.json()

    df = pd.DataFrame(data["symbols"])

    # 只保留正在交易的 perpetual contracts
    df = df[
        (df["contractType"] == "PERPETUAL") &
        (df["status"] == "TRADING")
    ].copy()

    dfasset = df[[
        "symbol",
        "quoteAsset",
        "marginAsset"
    ]]

    return dfasset


def merge_perp_data(dfvol, dfbidask, dfasset):
    df_all = dfasset.merge(
        dfvol,
        on="symbol",
        how="left"
    )

    df_all = df_all.merge(
        dfbidask,
        on="symbol",
        how="left"
    )

    return df_all


async def pricemap():
    url = "https://api.binance.com/api/v3/ticker/24hr"
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.get(url, timeout=20)
        resp.raise_for_status()
        data = resp.json()

    df = pd.DataFrame(data)

    col = "weightedAvgPrice"
    df[col] = pd.to_numeric(df[col], errors="coerce")

    price_map = dict(zip(df["symbol"], df[col]))

    return price_map


async def coinbase_usdt_rates():
    url = "https://api.coinbase.com/v2/exchange-rates"
    params = {
        "currency": "USDT"
    }
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.get(url, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()

    rates = data["data"]["rates"]

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

    direct_symbol = asset + "USDT"

    if direct_symbol in price_map:
        price = price_map[direct_symbol]

        if pd.notna(price) and price != 0:
            return float(price)

    inverse_symbol = "USDT" + asset

    if inverse_symbol in price_map:
        inverse_price = price_map[inverse_symbol]

        if pd.notna(inverse_price) and inverse_price != 0:
            return 1 / float(inverse_price)

    if asset in coinbase_rates:
        usdt_to_asset = coinbase_rates[asset]

        if usdt_to_asset != 0:
            return 1 / usdt_to_asset

    return None


async def convert_to_usdt(df_all):
    try:
        price_map = await pricemap()
    except Exception as e:
        price_map = {}
        print(f"Warning: failed to load binance fallback rates: {e}")

    try:
        coinbase_rates = await coinbase_usdt_rates()
    except Exception as e:
        coinbase_rates = {}
        print(f"Warning: failed to load Coinbase fallback rates: {e}")

    df_all = df_all.copy()

    df_all["rate_to_usdt"] = df_all["quoteAsset"].apply(
        lambda asset: get_rate_to_usdt(asset, price_map, coinbase_rates)
    )

    df_all["volume_usdt"] = df_all["quoteVolume"] * df_all["rate_to_usdt"]
    df_all["bidprice_usdt"] = df_all["bidPrice"] * df_all["rate_to_usdt"]
    df_all["askprice_usdt"] = df_all["askPrice"] * df_all["rate_to_usdt"]

    return df_all


def add_run_time(df_selected):
    df_selected = df_selected.copy()
    run_date_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    df_selected["run_date_utc"] = run_date_utc
    return df_selected

async def safe_fetch(name, coro, default_df):
    try:
        return await coro
    except Exception as e:
        print(f"{name} failed: {e}")
        return default_df.copy()
    
async def main():
    empty_vol = pd.DataFrame(columns=[
        "symbol",
        "quoteVolume",
        "volume",
        "weightedAvgPrice"
    ])

    empty_bidask = pd.DataFrame(columns=[
        "symbol",
        "bidPrice",
        "askPrice"
    ])

    empty_asset = pd.DataFrame(columns=[
        "symbol",
        "quoteAsset",
        "marginAsset"
    ])

    dfvol, dfbidask, dfasset = await asyncio.gather(
        safe_fetch(
            "quote_volume",
            fetchbinance_perp_quote_volume(),
            empty_vol
        ),
        safe_fetch(
            "bid_ask",
            fetchbinance_perp_bid_ask(),
            empty_bidask
        ),
        safe_fetch(
            "exchange_info",
            fetchperp_exchangeinfo(),
            empty_asset
        )
    )
    df_all = merge_perp_data(dfvol, dfbidask, dfasset)

    df_final = await convert_to_usdt(df_all)

    df_final = add_run_time(df_final)

    print(df_final[[
        "symbol",
        "quoteAsset",
        "marginAsset",
        "quoteVolume",
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
    print(missing_ticker[[
        "symbol",
        "quoteAsset",
        "marginAsset",
    ]])

    failed = df_final[
        (df_final["rate_to_usdt"].isna()) &
        (df_final["quoteVolume"] > 0)
    ]

    print("Failed to convert:")
    print(failed[[
        "symbol",
        "quoteAsset",
        "quoteVolume"
    ]])

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    output_file = f"binance_perp_volume_usdt_{timestamp}.csv"

    df_final.to_csv(output_file, index=False)

    print(f"Saved to {output_file}")


if __name__ == "__main__":
    asyncio.run(main())
