import pandas as pd
from datetime import datetime, timezone
import httpx
import asyncio


BYBIT_BASE_URL = "https://api.bybit.com"


async def bybit_get(path, params=None):
    url = BYBIT_BASE_URL + path

    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.get(url, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()

    if data.get("retCode") != 0:
        raise RuntimeError(
            f"Bybit API error: retCode={data.get('retCode')}, retMsg={data.get('retMsg')}"
        )

    return data["result"]


async def fetchbybit_perp_quote_volume_bid_ask_price():
    all_data = []

    for category in ["linear", "inverse"]:
        url_path = "/v5/market/tickers"

        params = {
            "category": category
        }

        result = await bybit_get(url_path, params=params)

        data = result["list"]

        df = pd.DataFrame(data)

        df["category"] = category

        all_data.append(df)

    df = pd.concat(all_data, ignore_index=True)

    numeric_cols = [
        "turnover24h",
        "bid1Price",
        "ask1Price",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    dfvol = df[[
        "category",
        "symbol",
        "turnover24h",
        "bid1Price",
        "ask1Price",
    ]].rename(columns={
        "turnover24h": "quoteVolume",
        "bid1Price": "bidPrice",
        "ask1Price": "askPrice"
    })

    return dfvol


async def fetchperp_exchangeinfo():
    all_data = []

    for category in ["linear", "inverse"]:
        cursor = None

        while True:
            url_path = "/v5/market/instruments-info"

            params = {
                "category": category,
                "status": "Trading",
                "limit": 1000
            }

            if cursor:
                params["cursor"] = cursor

            result = await bybit_get(url_path, params=params)

            data = result["list"]

            for row in data:
                row["category"] = category

            all_data.extend(data)

            cursor = result.get("nextPageCursor")

            if not cursor:
                break

    df = pd.DataFrame(all_data)

    df = df.drop_duplicates(subset=["category", "symbol"])

    # 只保留 perpetual，排除 futures
    df = df[
        df["contractType"].isin([
            "LinearPerpetual",
            "InversePerpetual"
        ])
    ].copy()

    df["maxLeverage"] = df["leverageFilter"].apply(
        lambda x: x.get("maxLeverage") if isinstance(x, dict) else None
    )

    df["maxLeverage"] = pd.to_numeric(df["maxLeverage"], errors="coerce")

    dfasset = df[[
        "category",
        "symbol",
        "baseCoin",
        "quoteCoin",
        "settleCoin",
        "contractType",
        "status",
        "maxLeverage"
    ]].rename(columns={
        "baseCoin": "baseAsset",
        "quoteCoin": "quoteAsset",
        "settleCoin": "marginAsset"
    })

    return dfasset


def merge_perp_data(dfvol, dfasset):
    df_all = dfasset.merge(
        dfvol,
        on=["category", "symbol"],
        how="left"
    )

    return df_all


async def pricemap():
    url_path = "/v5/market/tickers"

    params = {
        "category": "spot"
    }

    result = await bybit_get(url_path, params=params)

    data = result["list"]

    df = pd.DataFrame(data)

    df["lastPrice"] = pd.to_numeric(df["lastPrice"], errors="coerce")

    price_map = dict(zip(df["symbol"], df["lastPrice"]))

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

    # inverse perpetual 常见 quoteAsset 是 USD
    # 这里为了统一 quoteVolume 口径，近似按 1 USD = 1 USDT 处理
    if asset == "USD":
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
        print(f"Warning: failed to load Bybit fallback rates: {e}")

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
        "category",
        "symbol",
        "quoteVolume",
        "bidPrice",
        "askPrice",
    ])

    empty_asset = pd.DataFrame(columns=[
        "category",
        "symbol",
        "baseAsset",
        "quoteAsset",
        "marginAsset",
        "contractType",
        "status",
        "maxLeverage"
    ])

    dfvol, dfasset = await asyncio.gather(
        safe_fetch(
            "quote_volume_bid_ask_price",
            fetchbybit_perp_quote_volume_bid_ask_price(),
            empty_vol
        ),
        safe_fetch(
            "exchange_info",
            fetchperp_exchangeinfo(),
            empty_asset
        )
    )

    df_all = merge_perp_data(dfvol, dfasset)

    df_final = await convert_to_usdt(df_all)

    df_final = add_run_time(df_final)

    print(df_final[[
        "category",
        "symbol",
        "baseAsset",
        "quoteAsset",
        "marginAsset",
        "contractType",
        "status",
        "quoteVolume",
        "rate_to_usdt",
        "volume_usdt",
        "bidPrice",
        "askPrice",
        "bidprice_usdt",
        "askprice_usdt",
        "maxLeverage",
        "run_date_utc"
    ]].head())

    missing_ticker = df_final[df_final["quoteVolume"].isna()]

    print("Missing ticker data:")
    print(missing_ticker[[
        "category",
        "symbol",
        "baseAsset",
        "quoteAsset",
        "marginAsset",
        "contractType",
        "status"
    ]])

    failed = df_final[
        (df_final["rate_to_usdt"].isna()) &
        (df_final["quoteVolume"] > 0)
    ]

    print("Failed to convert:")
    print(failed[[
        "category",
        "symbol",
        "quoteAsset",
        "quoteVolume"
    ]])

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    output_file = f"bybit_trading_perp_volume_usdt_{timestamp}.csv"

    df_final.to_csv(output_file, index=False)

    print(f"Saved to {output_file}")


if __name__ == "__main__":
    asyncio.run(main())