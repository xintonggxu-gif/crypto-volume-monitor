#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import requests
import pandas as pd

def fetchbinance():
    url = 'https://api.binance.com/api/v3/ticker/24hr'
    params = {
        "type" : "MINI"
        }
    resp = requests.get(url, params, timeout = 20)
    resp.raise_for_status()
    data = resp.json()
    df = pd.DataFrame(data)
    col = 'volume'
    df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.rename(columns = {
        col:'volume24h'
        })
    df["exchange"] = "binance"
    dfvol = df[['exchange', 'symbol', 'volume24h']]
    print(dfvol.head())
    return dfvol

def fetchbybit():
    url = 'https://api.bybit.com/v5/market/tickers'
    params = {
        "category" : "spot"
        }
    resp = requests.get(url, params, timeout = 20)
    resp.raise_for_status()
    data = resp.json()
    df = pd.DataFrame(data['result']['list'])
    col = "volume24h"
    df[col] = pd.to_numeric(df[col], errors="coerce")
    df["exchange"] = "bybit"
    dfvol = df[["exchange", 'symbol', 'volume24h']]
    
    print(dfvol.head())
    return dfvol

def fetchokx():
    url = 'https://www.okx.com/api/v5/market/tickers'
    params = {
        'instType':'SPOT'
        }
    resp = requests.get(url, params, timeout = 20)
    resp.raise_for_status()
    data = resp.json()
    df = pd.DataFrame(data['data'])   
    col = "vol24h"
    df[col] = pd.to_numeric(df[col], errors="coerce")
    df['instId'] = df['instId'].str.replace('-', '', regex=False)
    df = df.rename(columns = {
        'instId':'symbol', 
        col:'volume24h'
        })
    
    df["exchange"] = "okx"
    dfvol = df[["exchange", 'symbol', 'volume24h']]
    print(dfvol.head())
    return dfvol

def fetchhashkey():
    url = 'https://api-glb.hashkey.com/quote/v1/ticker/24hr'
    params = {
        'instType': 'SPOT'
    }
    resp = requests.get(url, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    # 有些接口直接返回 list，稳一点写法
    if isinstance(data, dict) and 'data' in data:
        data = data['data']

    df = pd.DataFrame(data)
    col = 'v'
    df[col] = pd.to_numeric(df[col], errors='coerce')
    df['s'] = df['s'].str.replace('-', '', regex=False)
    df = df.rename(columns = {
        's':'symbol',
        col:'volume24h'
        })
    df["exchange"] = "hashkey"
    dfvol = df[['exchange', 'symbol', 'volume24h']]
    print(dfvol.head())
    return dfvol


def fetchkraken():
    url = 'https://api.kraken.com/0/public/Ticker'
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    if data['error']:
        raise ValueError(data['error'])

    df = pd.DataFrame(data['result']).T
    df = df.reset_index().rename(columns={'index': 'symbol'})

    # Kraken 的 v 是一个长度为 2 的数组：
    # v[0] = today, v[1] = last_24h
    col = 'volume24h'
    df[col] = pd.to_numeric(df['v'].str[1], errors='coerce')
    df["exchange"] = "kraken"
    dfvol = df[['exchange','symbol', 'volume24h']]
    print(dfvol[:10])
    return dfvol


def fetchcoinbase():
    url = 'https://api.exchange.coinbase.com/products/volume-summary'
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    # 官方文档页示例有嵌套 list，稳一点处理
    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], list):
        data = data[0]

    df = pd.DataFrame(data)
    col = 'spot_volume_24hour'
    df[col] = pd.to_numeric(df[col], errors='coerce')
    df['id'] = df['id'].str.replace('-', '', regex=False)
    df = df.rename(columns = {
        'id':'symbol',
        col:'volume24h'
        })
    df["exchange"] = "coinbase"
    dfvol = df[['exchange', 'symbol', 'volume24h']]
    print(dfvol.head())
    return dfvol


def fetchbitget():
    url = 'https://api.bitget.com/api/v2/spot/market/tickers'
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    df = pd.DataFrame(data['data'])

    col = 'baseVolume'
    df[col] = pd.to_numeric(df[col], errors='coerce')
    df = df.rename(columns = {
        col:'volume24h'
        })
    df["exchange"] = "bitget"
    dfvol = df[['exchange', 'symbol', 'volume24h']]
    print(dfvol.head())
    return dfvol

def fetch_all():
    dfs = [
        fetchbinance(),
        fetchbybit(),
        fetchokx(),
        fetchhashkey(),
        fetchkraken(),
        fetchcoinbase(),
        fetchbitget(),
    ]
    return pd.concat(dfs, ignore_index=True)

all_df = fetch_all()
print(all_df.head())
print(all_df.shape)
all_df.to_csv("all_exchanges_volume.csv", index=False)
    