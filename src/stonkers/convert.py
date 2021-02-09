#
# Convert API responses to Pandas DataFrames
#

import pandas as pd


def accounts(data):
    """accounts as dataframe"""
    return pd.concat(
        pd.json_normalize(v["securitiesAccount"]) for v in data.values()
    ).set_index("accountId")


def transactions(data):
    """transaction information as Dataframe"""
    return pd.json_normalize(data)


def search(data):
    """search for symbol as a dataframe"""
    ret = []
    for symbol in data:
        ret.append(data[symbol])

    return pd.DataFrame(ret)


def instrument(data):
    """instrument info from cusip as dataframe"""
    return pd.DataFrame(data)


def quote(data):
    """quote as dataframe"""
    return pd.DataFrame(data).T.set_index("symbol")


def history(data):
    """get history as dataframe"""
    df = pd.DataFrame(data["candles"])
    df["datetime"] = pd.to_datetime(df["datetime"], unit="ms")
    return df


def options(data):
    """options chain as dataframe"""
    ret = []
    for date in data["callExpDateMap"]:
        for strike in data["callExpDateMap"][date]:
            ret.extend(data["callExpDateMap"][date][strike])
    for date in data["putExpDateMap"]:
        for strike in data["putExpDateMap"][date]:
            ret.extend(data["putExpDateMap"][date][strike])

    df = pd.DataFrame(ret)
    for col in (
        "tradeTimeInLong",
        "quoteTimeInLong",
        "expirationDate",
        "lastTradingDay",
    ):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], unit="ms")

    for col in ("delta", "gamma", "theta", "vega", "rho", "volatility"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def positions(data):
    """positions list as a dataframe"""
    ret = []

    for position in data:
        instrument = position.pop("instrument", {})
        for col in ("assetType", "cusip", "symbol"):
            if col in instrument:
                position[col] = instrument[col]

        ret.append(position)

    return pd.DataFrame(ret).set_index("symbol")
