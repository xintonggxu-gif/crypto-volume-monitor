#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import requests
import pandas as pd
from datetime import datetime, timezone

def fetchbinance():
    url = 'https://api.binance.com/api/v3/ticker/24hr'
    resp = requests.get(url, timeout = 20)
    resp.raise_for_status()
    data = resp.json()
    df = pd.DataFrame(data)
    col = 'quoteVolume'
    df[col] = pd.to_numeric(df[col], errors="coerce")
    dfvol = df[['symbol', 'quoteVolume']]
    
    return dfvol

def fetchquoteasset():
    url = 'https://api.binance.com/api/v3/exchangeInfo'
    resp = requests.get(url, timeout = 20)
    resp.raise_for_status()
    data = resp.json()
    df = pd.DataFrame(data["symbols"])
    dfasset = df[['symbol','quoteAsset']]
    return dfasset

          

def fetchsymbol(dfvol, dfasset):
    file_path = "/Users/karina/Downloads/Zero-Fees_Spot_Binance.xlsx"
    
    df = pd.read_excel(file_path, header=1)
    
    symbols = (
        df["Symbol"]
        .dropna()
        .astype(str)
        .str.strip()
        .drop_duplicates()
        .tolist()
    )

    rows = []

    for original_symbol in symbols:
        symbol = original_symbol

        binance_symbol = symbol.replace("/", "")

        rows.append({
            "original_symbol": original_symbol,
            "symbol": binance_symbol
        })

    # 关键：把 Excel 里的 symbols 做成 DataFrame
    df_symbols = pd.DataFrame(rows)

    # 防止右边有重复 symbol，merge 后行数变多
    dfvol = dfvol.drop_duplicates(subset=["symbol"])
    dfasset = dfasset.drop_duplicates(subset=["symbol"])

    # 以 Excel 表里的 symbol 为主，有数据就填，没有就 NaN
    df_selected = df_symbols.merge(
        dfvol,
        on="symbol",
        how="left"
    )

    df_selected = df_selected.merge(
        dfasset,
        on="symbol",
        how="left"
    )

    df_selected["found_in_binance"] = df_selected["quoteVolume"].notna()

    return df_selected
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
        
def get_rate_to_usdt(asset, price_map):
    if pd.isna(asset):
        return None
    
    if asset == "USDT":
        return 1.0

    # 例如 BTC -> BTCUSDT, FDUSD -> FDUSDUSDT
    direct_symbol = asset + "USDT"

    if direct_symbol in price_map:
        return price_map[direct_symbol]

    # 例如 TRY -> USDTTRY
    # USDTTRY = 32 表示 1 USDT = 32 TRY
    # 所以 1 TRY = 1 / 32 USDT
    inverse_symbol = "USDT" + asset

    if inverse_symbol in price_map:
        inverse_price = price_map[inverse_symbol]

        if inverse_price != 0:
            return 1 / inverse_price

    return None

def convert_to_usdt(df_selected):
    price_map = pricemap()

    df_selected = df_selected.copy()


    df_selected["rate_to_usdt"] = df_selected["quoteAsset"].apply(
        lambda asset: get_rate_to_usdt(asset, price_map)
    )

    df_selected["volume_usdt"] = (
        df_selected["quoteVolume"] * df_selected["rate_to_usdt"]
    )

    return df_selected    

def add_run_time(df_selected): 
    df_selected  = df_selected.copy()
    run_date_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    df_selected ["run_date_utc"] = run_date_utc
    return df_selected    
    
def main():
    dfvol = fetchbinance()

    dfasset = fetchquoteasset()

    df_selected = fetchsymbol(dfvol, dfasset)

    df_final = convert_to_usdt(df_selected)

    # 加 run date
    df_final = add_run_time(df_final)

    print(df_final[[
        "original_symbol",
        "symbol",
        "quoteVolume",
        "quoteAsset",
        "found_in_binance",
        "rate_to_usdt",
        "volume_usdt",
        "run_date_utc"
    ]].head())

    failed = df_final[df_final["rate_to_usdt"].isna()]

    print("Failed to convert:")
    print(failed[["original_symbol", "symbol", "quoteAsset", "quoteVolume"]])

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    output_file = f"binance_volume_usdt_{timestamp}.csv"

    df_final.to_csv(output_file, index=False)

    print(f"Saved to {output_file}")


if __name__ == "__main__":
    main()   
    
    
    
    
