#
# Convert API responses to Pandas DataFrames
#

from typing import Optional

import pandas as pd


def parse_ticker(ticker: str) -> Optional[dict]:
    """
    Parse a ticker string into a dictionary of values.
    """
    # Check if "_" exists in the ticker, indicating it's an option
    if "_" not in ticker:
        return {
            "asset_type": "EQUITY",
            "symbol": ticker,
            "underlying": ticker,
        }

    # Split the ticker on "_"
    parts = ticker.split("_")
    if len(parts) != 2:
        return None  # Unrecognized format

    underlying = parts[0]
    remainder = parts[1]

    # Identify the ContractType and split the rest of the string accordingly
    if "P" in remainder:
        contract_type = "PUT"
        parts = remainder.split("P", 1)

    elif "C" in remainder:
        contract_type = "CALL"
        parts = remainder.split("C", 1)

    else:
        return None  # Unrecognized format

    # Extract expiration and strike
    expiration = parts[0]
    strike = parts[1]

    return {
        "asset_type": "OPTION",
        "underlying": underlying,
        "symbol": ticker,
        "expiration": expiration,
        "contract_type": contract_type,
        "strike": strike,
    }


def accounts(data):
    """accounts as dataframe"""
    return pd.json_normalize(data.values()).set_index("accountId")


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
        instrument_position = position.pop("instrument", {})
        for col in ("assetType", "cusip", "symbol"):
            if col in instrument_position:
                position[col] = instrument_position[col]

        position.update(parse_ticker(position["symbol"]))

        if "expiration" in position:
            position["expiration_date"] = pd.to_datetime(
                position["expiration"], format="%m%d%y"
            )

        if "strike" in position:
            position["strike"] = pd.to_numeric(position["strike"])

        ret.append(position)

    return pd.DataFrame(ret).set_index("symbol")


def user_principals(data):
    return pd.json_normalize(data)
