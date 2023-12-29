#

import asyncio
import dataclasses
from typing import Any, Callable, Iterable, List, Optional

import pandas as pd
import rich
import rich.align
import rich.console
import rich.live
import rich.panel
import rich.progress
import rich.spinner
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
    options = positions[positions["assetType"] == "OPTION"]
    equities = positions[positions["assetType"] == "EQUITY"]

    # Helper function to check for existing short options
    def has_short_option(
        option_type: tda.client.Client.Options.ContractType,
    ) -> bool:
        # Filter the DataFrame for the specific ticker
        filtered = options.loc[
            options.index.get_level_values("underlying") == ticker
        ]

        # Apply the conditions on the filtered DataFrame
        relevant_option = filtered[
            (filtered.index.get_level_values(1).str.contains(f"{ticker}_"))
            & (
                filtered.index.get_level_values(1).str[10]
                == option_type.value[0]
            )
            & (filtered["shortQuantity"] > 0)
        ]
        return not relevant_option.empty

    # 1. Check if you have a short PUT for the ticker.
    if has_short_option(tda.client.Client.Options.ContractType.PUT):
        return f"Already have a short PUT for {ticker}."

    contract_type = tda.client.Client.Options.ContractType.PUT

    # 2. Check if you have the underlying stock.
    if ticker in equities.index.get_level_values("underlying"):
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
        long_quantity = 0

        if (ticker, ticker) in equities.index:
            value = positions.loc[(ticker, ticker), "longQuantity"]
            long_quantity = int(pd.to_numeric(value, errors="coerce"))

        num_contracts = int(long_quantity / option["multiplier"])

    return (
        f"Sell {num_contracts} x "
        f"{option['description']} for {option['mark']} "
        "for a total of "
        f"${num_contracts * option['mark'] * option['multiplier']:.2f}. "
        f"with a RoR of {option['RoR']:.2%}."
    )


def number(
    value: float,
    precision: int = 2,
    percent: bool = False,
    currency: str = "",
    bold: bool = True,
) -> str:
    color = "green" if value >= 0 else "red"
    bolded = "bold " if bold else ""

    def fmt(value: float, pre: str = "", post: str = "") -> str:
        value_str = f"{value:,.{precision}f}"

        if precision > 2:
            # NOTE(jkoelker) Remove all trailing zeros.
            value_str = value_str.rstrip("0")

            # NOTE(jkoelker) If the last character is a decimal point,
            # pad up to 2 decimal places with zeros.
            while len(value_str.split(".")[-1]) < 2:
                value_str += "0"

        return f"[{bolded}{color}]{pre}{value_str}{post}[/{bolded}{color}]"

    if percent:
        value *= 100

    return fmt(
        value, pre=currency if currency else "", post="%" if percent else ""
    )


@dataclasses.dataclass
class AccountSummary:
    account_id: str
    client: Client
    margin_usage: float = 0.5
    update: Optional[Callable[[], None]] = None

    _account: Optional[pd.DataFrame] = None
    _loading = rich.spinner.Spinner("dots", "Loading account summary...")

    def __rich__(self):
        def panel(renderable: rich.console.RenderableType) -> rich.panel.Panel:
            return rich.panel.Panel(
                renderable,
                title="Account summary",
            )

        if self._account is None:
            return panel(rich.align.Align.center(self._loading))

        summary = rich.table.Table(
            show_header=False,
            expand=True,
        )

        summary.add_row(
            "Net liquidation",
            number(self.net_liquidation, currency="$"),
            "Buying power",
            number(self.buying_power, currency="$"),
        )
        summary.add_row(
            "Maintenance requirement",
            number(self.maintenance_requirement, currency="$"),
            "Available funds",
            number(self.available_funds, currency="$"),
        )

        summary.add_row(
            "Round trips",
            number(self.round_trips, precision=0),
            "Day Trades Left",
            number(3 - self.round_trips, precision=0),
        )

        summary.add_section()

        summary.add_row(
            "Target buying power usage",
            number(self.target_buying_power, currency="$"),
        )

        return panel(summary)

    def account_get(self, key: str, balance_type: str = "") -> Any:
        if balance_type:
            key = f"{balance_type}.{key}"

        return self.account[key][self.account_id]

    @property
    def account(self) -> pd.DataFrame:
        if self._account is None:
            raise RuntimeError("Account summary has not been fetched yet.")

        return self._account

    @property
    def target_buying_power(self) -> float:
        return self.net_liquidation * self.margin_usage

    @property
    def available_funds(self) -> float:
        return self.account_get("availableFunds", "currentBalances")

    @property
    def display_name(self) -> str:
        return self.account_get("displayName")

    @property
    def buying_power(self) -> float:
        return self.account_get("buyingPower", "currentBalances")

    @property
    def is_day_trader(self) -> bool:
        return self.account_get("isDayTrader")

    @property
    def maintenance_requirement(self) -> float:
        return self.account_get("maintenanceRequirement", "currentBalances")

    @property
    def money_market_fund(self) -> float:
        return self.account_get("moneyMarketFund", "currentBalances")

    @property
    def net_liquidation(self) -> float:
        return self.account_get("liquidationValue", "currentBalances")

    @property
    def round_trips(self) -> int:
        return self.account_get("roundTrips")

    @property
    def savings(self) -> float:
        return self.account_get("savings", "currentBalances")

    @property
    def type(self) -> str:
        return self.account_get("type")

    def _update(self):
        if self.update is not None:
            self.update()

    async def __call__(self):
        self._account = await self.client.account(self.account_id)
        self._update()


@dataclasses.dataclass
class Ticker:
    ticker: str
    update: Optional[Callable[[], None]] = None
    _status: str = ""
    _loading = rich.spinner.Spinner("dots", "Loading...")

    def __rich__(self):
        if self._status == "":
            return rich.align.Align.center(self._loading)

        return self._status

    @property
    def status(self) -> str:
        if self._status == "":
            raise RuntimeError("Ticker status has not been fetched yet.")

        return self._status

    def _update(self):
        if self.update is not None:
            self.update()

    async def __call__(self, client: Client, positions: pd.DataFrame):
        result = await wheel(client, positions, self.ticker)
        self._status = result
        self._update()


@dataclasses.dataclass
class Positions:
    update: Optional[Callable[[], None]] = None
    _positions: Optional[pd.DataFrame] = None
    _quotes: Optional[pd.DataFrame] = None
    _loading = rich.spinner.Spinner("dots", "Loading positions...")

    def __rich__(self):
        def panel(renderable: rich.console.RenderableType) -> rich.panel.Panel:
            return rich.panel.Panel(
                renderable,
                title="Positions",
            )

        if self._positions is None or self._quotes is None:
            return panel(rich.align.Align.center(self._loading))

        table = rich.table.Table(
            expand=True,
        )

        table.add_column("Symbol")
        table.add_column("R")
        table.add_column("Qty", justify="right")
        table.add_column("MktPrice", justify="right")
        table.add_column("AvgPrice", justify="right")
        table.add_column("Value", justify="right")
        table.add_column("Cost", justify="right")
        table.add_column("Unrealized P&L", justify="right")
        table.add_column("P&L", justify="right")
        table.add_column("Strike", justify="right")
        table.add_column("Exp", justify="right")
        table.add_column("DTE", justify="right")
        table.add_column("ITM?")

        def cost_basis(row: pd.Series) -> float:
            quantity = row["longQuantity"] - row["shortQuantity"]
            value = row["averagePrice"] * quantity

            if row["assetType"] == "OPTION":
                value = value * 100

            return value

        def is_in_the_money(mark: float, row: pd.Series) -> bool:
            if row["assetType"] != "OPTION":
                return False

            if row["contract_type"] == "CALL":
                return row["strike"] > mark

            return row["strike"] < mark

        for ticker in self.positions.index.get_level_values(
            "underlying"
        ).unique():
            positions = self.positions.loc[ticker]

            table.add_row(ticker)

            for symbol, pos in positions.iterrows():
                right = (
                    "S"
                    if pos["assetType"] == "EQUITY"
                    else pos["contract_type"][0]
                )
                mark = self.quotes.loc[symbol]["mark"]
                cost = cost_basis(pos)
                profit = pos["marketValue"] - cost
                profit_percent = profit / abs(cost)

                row = (
                    "",
                    right,
                    number(
                        pos["longQuantity"] - pos["shortQuantity"], precision=0
                    ),
                    number(mark, currency="$", precision=5),
                    number(pos["averagePrice"], currency="$", precision=5),
                    number(pos["marketValue"], currency="$", precision=5),
                    number(cost, currency="$", precision=5),
                    number(profit, currency="$", precision=5),
                    number(profit_percent, percent=True, precision=2),
                )

                if pos["assetType"] == "OPTION":
                    dte = pos["expiration_date"] - pd.Timestamp.now()
                    row += (
                        number(pos["strike"], currency="$", precision=5),
                        pos["expiration_date"].strftime("%Y-%m-%d"),
                        number(dte.days, precision=0),
                        ":heavy_check_mark:"
                        if is_in_the_money(mark, pos)
                        else "",
                    )

                table.add_row(*row)

            table.add_section()

        return panel(table)

    def _update(self):
        if self.update is not None:
            self.update()

    @property
    def equities(self) -> pd.DataFrame:
        return self.positions[self.positions["assetType"] == "EQUITY"]

    @property
    def options(self) -> pd.DataFrame:
        return self.positions[self.positions["assetType"] == "OPTION"]

    @property
    def positions(self) -> pd.DataFrame:
        if self._positions is None:
            raise RuntimeError("Positions have not been fetched yet.")

        return self._positions

    @property
    def quotes(self) -> pd.DataFrame:
        if self._quotes is None:
            raise RuntimeError("Quotes have not been fetched yet.")

        return self._quotes

    async def __call__(self, client: Client, account_id: str):
        positions = await client.positions(account_id)
        positions.set_index(["underlying", positions.index], inplace=True)

        # sort by underlying, then by expiration_date
        positions.sort_values(
            ["underlying", "contract_type", "expiration_date"],
            inplace=True,
            na_position="first",
        )

        self._positions = positions

        symbols = positions.index.get_level_values("symbol").tolist()
        underlying = positions.index.get_level_values("underlying").tolist()
        tickers = set(symbols + underlying)

        self._quotes = await client.quote(tickers)

        self._update()


async def wheelie(
    client: Client,
    account_id: str,
    tickers: Iterable[str],
):
    console = rich.console.Console()

    with rich.live.Live(console=console, auto_refresh=False) as live:
        account = AccountSummary(
            account_id,
            client,
            update=lambda: live.update(account),
        )
        await account()

    with rich.live.Live(console=console) as live:
        positions = Positions(lambda: live.update(positions))
        await positions(client, account_id)
        live.update(positions)

    with rich.live.Live(console=console, auto_refresh=False) as live:
        table = rich.table.Table()

        table.add_column("Ticker", justify="left", style="cyan")
        table.add_column("Status", justify="left", style="green")

        def update():
            live.update(table)

        rich_tickers = [Ticker(ticker, update) for ticker in tickers]

        for ticker in rich_tickers:
            table.add_row(ticker.ticker, ticker)

        tasks = [
            ticker(client, positions.positions) for ticker in rich_tickers
        ]
        await asyncio.gather(*tasks)
