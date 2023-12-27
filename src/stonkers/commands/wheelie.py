#

import asyncio
from typing import Callable, Iterable, List, Optional

import pandas as pd
import rich
import rich.console
import rich.live
import rich.table
import tda  # type: ignore

from ..client import Client


def add_rate_of_return(options_df: pd.DataFrame) -> pd.DataFrame:
    """Add the rate of return to the options DataFrame."""
    if "RoR" not in options_df.columns:
        # Calculate RoR using the 'mark' as the estimated premium
        options_df["RoR"] = options_df["mark"] / (
            options_df["strikePrice"] - options_df["mark"]
        )

    return options_df


def rate_of_return_condition(
    rate_of_return: float = 0.02,
) -> Callable[[pd.DataFrame], pd.Series]:
    def _condition(df: pd.DataFrame) -> pd.Series:
        df = add_rate_of_return(df)
        return df["RoR"] > rate_of_return

    return _condition


def open_interest_condition(
    open_interest: int = 100,
) -> Callable[[pd.DataFrame], pd.Series]:
    return lambda df: df["openInterest"] > open_interest


def total_volume_condition(
    total_volume: int = 50,
) -> Callable[[pd.DataFrame], pd.Series]:
    return lambda df: df["totalVolume"] > total_volume


def days_to_expiration_condition(
    min_days: int = 7,
    max_days: int = 60,
) -> Callable[[pd.DataFrame], pd.Series]:
    return lambda df: df["daysToExpiration"].between(min_days, max_days)


def spread_condition(
    spread: float = 0.10,
) -> Callable[[pd.DataFrame], pd.Series]:
    def _condition(df: pd.DataFrame) -> pd.Series:
        if "spread" not in df.columns:
            df["spread"] = df["ask"] - df["bid"]

        return df["spread"] < spread

    return _condition


def exclude_in_the_money_condition() -> Callable[[pd.DataFrame], pd.Series]:
    return lambda df: ~df["inTheMoney"]


def delta_target_condition(
    target_delta: float = 0.30,
) -> Callable[[pd.DataFrame], pd.Series]:
    return lambda df: df["delta"].abs() <= target_delta


def delta_range_condition(
    target_delta: float = 0.30,
    tolerance: float = 0.05,
) -> Callable[[pd.DataFrame], pd.Series]:
    return lambda df: df["delta"].between(
        target_delta - tolerance, target_delta + tolerance
    )


def intrinsic_value_condition(
    min_value: float = 0.0,
) -> Callable[[pd.DataFrame], pd.Series]:
    return lambda df: df["intrinsicValue"] >= min_value


def liquidity_condition(
    min_bid_size: int = 10,
    min_ask_size: int = 10,
) -> Callable[[pd.DataFrame], pd.Series]:
    return lambda df: (df["bidSize"] >= min_bid_size) & (
        df["askSize"] >= min_ask_size
    )


def contract_condition(
    contract_type: str = tda.client.Client.Options.ContractType.PUT,
) -> Callable[[pd.DataFrame], pd.Series]:
    if isinstance(contract_type, tda.client.Client.Options.ContractType):
        contract_type = contract_type.value

    return lambda df: df["putCall"] == contract_type


def combined_condition(
    df: pd.DataFrame,
    *conditions: Callable[[pd.DataFrame], pd.Series],
) -> pd.Series:
    if not conditions:
        return pd.Series([True] * len(df), index=df.index)

    combined = conditions[0](df)

    for condition in conditions[1:]:
        combined &= condition(df)

    return combined


def default_conditions() -> List[Callable[[pd.DataFrame], pd.Series]]:
    return [
        days_to_expiration_condition(),
        spread_condition(),
        exclude_in_the_money_condition(),
        delta_target_condition(),
    ]


def evaluate_options(
    options_df: pd.DataFrame,
    filter_conditions: Optional[
        Iterable[Callable[[pd.DataFrame], pd.Series]]
    ] = None,
) -> pd.DataFrame:
    """Evaluate options using the provided pandas DataFrame keys."""

    if options_df.empty:
        return options_df

    # Copy the dataframe to avoid modifying the original one
    options_df = options_df.copy()

    if filter_conditions is None:
        filter_conditions = default_conditions()

    options_df = add_rate_of_return(options_df)

    # Apply the default filter conditions
    condition = combined_condition(options_df, *filter_conditions)

    return options_df[condition].sort_values(by="RoR", ascending=False)


def best_option(
    options_df: pd.DataFrame,
    filter_conditions: Optional[
        Iterable[Callable[[pd.DataFrame], pd.Series]]
    ] = None,
) -> Optional[pd.Series]:
    """Retrieve the best option based on the evaluation criteria."""
    evaluated_df = evaluate_options(
        options_df, filter_conditions=filter_conditions
    )
    return evaluated_df.iloc[0] if not evaluated_df.empty else None


async def wheel(client: Client, positions: pd.DataFrame, ticker: str) -> str:
    # Helper function to check for existing short options
    def has_short_option(
        option_type: tda.client.Client.Options.ContractType,
    ) -> bool:
        relevant_option = positions[
            (positions.index.str.contains(f"{ticker}_"))
            & (positions.index.str[10] == option_type.value[0])
            & (positions["shortQuantity"] > 0)
        ]
        return not relevant_option.empty

    # 1. Check if you have a short PUT for the ticker.
    if has_short_option(tda.client.Client.Options.ContractType.PUT):
        return f"Already have a short PUT for {ticker}."

    contract_type = tda.client.Client.Options.ContractType.PUT

    # 2. Check if you have the underlying stock.
    if ticker in positions.index:
        # Check if we have already sold covered calls
        if has_short_option(tda.client.Client.Options.ContractType.CALL):
            return f"Already have covered calls for {ticker}"

        # 3. Sell covered calls.
        contract_type = tda.client.Client.Options.ContractType.CALL

    # Fetch the options chain
    chain = await client.options(
        ticker,
        include_quotes=True,
        contract_type=contract_type,
        option_type=tda.client.Client.Options.Type.STANDARD,
    )

    filter_conditions = [
        contract_condition(contract_type),
    ] + default_conditions()

    # Evaluate the options
    option = best_option(chain, filter_conditions=filter_conditions)

    if option is None or option.empty:
        return f"No options found for {ticker}."

    num_contracts = 1
    if contract_type == tda.client.Client.Options.ContractType.CALL:
        num_contracts = int(
            positions.loc[ticker, "longQuantity"] / option["multiplier"]
        )

    return (
        f"Sell {num_contracts} x "
        f"{option['description']} for {option['mark']} "
        "for a total of "
        f"${num_contracts * option['mark'] * option['multiplier']:.2f}. "
        f"with a RoR of {option['RoR']:.2%}."
    )


class Ticker:
    def __init__(self, ticker: str, update: Callable[[], None]):
        self.ticker = ticker
        self.status = "Pending"
        self._update = update

    def __rich__(self):
        return self.status

    async def wheel(self, client: Client, positions: pd.DataFrame):
        result = await wheel(client, positions, self.ticker)
        self.status = result
        self._update()


async def wheelie(
    client: Client,
    account_id: str,
    tickers: Iterable[str],
):
    console = rich.console.Console()

    with console.status("[bold green]Fetching positions..."):
        positions = await client.positions(account_id)

    with rich.live.Live(console=console, auto_refresh=False) as live:
        table = rich.table.Table()

        table.add_column("Ticker", justify="left", style="cyan")
        table.add_column("Status", justify="left", style="green")

        def update():
            live.update(table)

        rich_tickers = [Ticker(ticker, update) for ticker in tickers]

        for ticker in rich_tickers:
            table.add_row(ticker.ticker, ticker)

        tasks = [ticker.wheel(client, positions) for ticker in rich_tickers]
        await asyncio.gather(*tasks)
