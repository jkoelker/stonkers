#

from typing import Callable, Dict, Iterable, List, Optional, TypeAlias

import numpy as np
import pandas as pd
import tda  # type: ignore

from .price import market_price

ContractType: TypeAlias = tda.client.Client.Options.ContractType


def _add_rate_of_return(options_df: pd.DataFrame) -> pd.DataFrame:
    """Add the rate of return to the options DataFrame."""
    if "RoR" not in options_df.columns:
        options_df = options_df.copy()

        options_df["marketPrice"] = options_df.apply(market_price, axis=1)
        options_df["RoR"] = options_df["marketPrice"] / (
            options_df["strikePrice"] - options_df["marketPrice"]
        )
    return options_df


def rate_of_return(
    value: float = 0.02,
) -> Callable[[pd.DataFrame], pd.Series]:
    def _condition(df: pd.DataFrame) -> pd.Series:
        df = _add_rate_of_return(df)
        return df["RoR"] > value

    return _condition


def minimum_price(
    value: float = 0.01,
) -> Callable[[pd.DataFrame], pd.Series]:
    return lambda df: df["mark"] > value


def maximum_price(
    value: float = 1000.0,
) -> Callable[[pd.DataFrame], pd.Series]:
    return lambda df: df["mark"] < value


def open_interest(
    value: int = 100,
) -> Callable[[pd.DataFrame], pd.Series]:
    return lambda df: df["openInterest"] > value


def total_volume(
    value: int = 50,
) -> Callable[[pd.DataFrame], pd.Series]:
    return lambda df: df["totalVolume"] > value


def days_to_expiration(
    min_days: int = 7,
    max_days: int = 60,
) -> Callable[[pd.DataFrame], pd.Series]:
    return lambda df: df["daysToExpiration"].between(min_days, max_days)


def spread(
    value: float = 0.05,
) -> Callable[[pd.DataFrame], pd.Series]:
    def _condition(df: pd.DataFrame) -> pd.Series:
        if "spread" not in df.columns:
            df = df.copy()
            df["spread"] = df["ask"] - df["bid"]

        return df["spread"] < value

    return _condition


def exclude_in_the_money() -> Callable[[pd.DataFrame], pd.Series]:
    return lambda df: ~df["inTheMoney"]


def delta(
    target: float = 0.30,
    tolerance: float = 0.05,
    ignore_negative: bool = True,
) -> Callable[[pd.DataFrame], pd.Series]:
    return lambda df: pd.Series(
        np.isclose(
            df["delta"].abs() if ignore_negative else df["delta"],
            target,
            atol=tolerance,
        ),
    )


def intrinsic_value(
    min_value: float = 0.0,
) -> Callable[[pd.DataFrame], pd.Series]:
    return lambda df: df["intrinsicValue"] >= min_value


def liquidity(
    min_bid_size: int = 10,
    min_ask_size: int = 10,
) -> Callable[[pd.DataFrame], pd.Series]:
    return lambda df: (df["bidSize"] >= min_bid_size) & (
        df["askSize"] >= min_ask_size
    )


def is_option_type(
    option_type: str | ContractType = ContractType.PUT,
) -> Callable[[pd.DataFrame], pd.Series]:
    if isinstance(option_type, tda.client.Client.Options.ContractType):
        option_type = option_type.value

    return lambda df: df["putCall"] == option_type


def is_call() -> Callable[[pd.DataFrame], pd.Series]:
    return is_option_type(tda.client.Client.Options.ContractType.CALL.value)


def is_put() -> Callable[[pd.DataFrame], pd.Series]:
    return is_option_type(tda.client.Client.Options.ContractType.PUT.value)


def combined(
    df: pd.DataFrame,
    *conditions: Callable[[pd.DataFrame], pd.Series],
) -> pd.Series:
    if not conditions:
        return pd.Series([True] * len(df), index=df.index)

    _combined = conditions[0](df)

    for condition in conditions[1:]:
        _combined &= condition(df)

    return _combined


def default() -> List[Callable[[pd.DataFrame], pd.Series]]:
    return [
        days_to_expiration(),
        spread(),
        exclude_in_the_money(),
        delta(),
    ]


def evaluate(
    options_df: pd.DataFrame,
    filter_conditions: Optional[
        Iterable[Callable[[pd.DataFrame], pd.Series]]
    ] = None,
    order_by: Optional[Dict[str, bool]] = None,
) -> pd.DataFrame:
    """Evaluate options using the provided pandas DataFrame keys."""

    if options_df.empty:
        return options_df

    # Copy the dataframe to avoid modifying the original one
    options_df = options_df.copy()

    if filter_conditions is None:
        filter_conditions = default()

    options_df = _add_rate_of_return(options_df)

    # Apply the default filter conditions
    condition = combined(options_df, *filter_conditions)

    if order_by is None:
        order_by = {"RoR": False, "expirationDate": True}

    valid_order_by = {
        key: order_by[key] for key in order_by if key in options_df.columns
    }

    return options_df[condition].sort_values(
        by=list(valid_order_by.keys()), ascending=list(valid_order_by.values())
    )


def best(
    options_df: pd.DataFrame,
    filter_conditions: Optional[
        Iterable[Callable[[pd.DataFrame], pd.Series]]
    ] = None,
    order_by: Optional[Dict[str, bool]] = None,
) -> Optional[pd.Series]:
    """Retrieve the best option based on the evaluation criteria."""
    evaluated_df = evaluate(
        options_df, filter_conditions=filter_conditions, order_by=order_by
    )
    return evaluated_df.iloc[0] if not evaluated_df.empty else None
