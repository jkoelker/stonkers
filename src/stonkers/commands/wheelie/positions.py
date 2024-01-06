#

import dataclasses
from typing import Callable, Iterable, Optional

import pandas as pd
import rich
import rich.align
import rich.console
import rich.live
import rich.panel
import rich.progress
import rich.spinner
import rich.table
from stonkers.client import Client

from .formatting import number
from .price import market_price


@dataclasses.dataclass
class Positions:
    tickers: Iterable[str]
    update: Optional[Callable[[], None]] = None
    _positions: Optional[pd.DataFrame] = None
    _quotes: Optional[pd.DataFrame] = None
    _is_loading: bool = True
    _loading = rich.spinner.Spinner("dots", "Loading positions...")

    def __rich__(self):
        def panel(renderable: rich.console.RenderableType) -> rich.panel.Panel:
            return rich.panel.Panel(
                renderable,
                title="Positions",
            )

        if self._is_loading:
            return panel(rich.align.Align.center(self._loading))

        table = rich.table.Table(
            expand=True,
        )

        if self.positions.empty or self.quotes.empty:
            return panel(table)

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

        def is_in_the_money(
            underlying_price: float,
            row: pd.Series,
        ) -> bool:
            if row["assetType"] != "OPTION":
                return False

            if row["contract_type"] == "CALL":
                return row["strike"] < underlying_price

            return row["strike"] > underlying_price

        def render_position(symbol: str, pos: pd.Series, underlying: str):
            right = (
                "S"
                if pos["assetType"] == "EQUITY"
                else pos["contract_type"][0]
            )
            mark = market_price(self.quotes.loc[symbol])
            cost = cost_basis(pos)
            profit = pos["marketValue"] - cost
            profit_percent = profit / abs(cost)

            row = [
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
            ]

            if pos["assetType"] == "OPTION":
                underlying_price = market_price(self.quotes.loc[underlying])

                dte = pos["expiration_date"] - pd.Timestamp.now()
                row += (
                    number(pos["strike"], currency="$", precision=5),
                    pos["expiration_date"].strftime("%Y-%m-%d"),
                    number(dte.days, precision=0),
                    ":heavy_check_mark:"
                    if is_in_the_money(underlying_price, pos)
                    else "",
                )

            table.add_row(*row)

        show_tickers = set(self.tickers)

        for ticker in self.positions.index.get_level_values(
            "underlying"
        ).unique():
            if ticker not in show_tickers:
                continue

            positions = self.positions.loc[ticker]

            table.add_row(ticker)

            for symbol, pos in positions.iterrows():
                render_position(symbol, pos, underlying=ticker)

            table.add_section()

        return panel(table)

    def _update(self):
        if self.update is not None:
            self.update()

    @staticmethod
    def _empty_postions() -> pd.DataFrame:
        symbols = pd.Index([], name="symbol", dtype="str")  # type: pd.Index
        underlying = pd.Index(
            [], name="underlying", dtype="str"
        )  # type: pd.Index

        positions = pd.DataFrame(
            {
                "assetType": [],
                "contract_type": [],
                "strike": [],
                "expiration_date": [],
                "longQuantity": [],
                "shortQuantity": [],
                "averagePrice": [],
                "marketValue": [],
            },
            index=pd.MultiIndex.from_arrays(
                [underlying, symbols],
                names=["underlying", "symbol"],
            ),
        )

        return positions

    @staticmethod
    def _empty_quotes() -> pd.DataFrame:
        return pd.DataFrame(
            {
                "symbol": [],
                "mark": [],
            }
        )

    @property
    def full_lot_equities(self) -> pd.DataFrame:
        return self.equities[
            (self.equities["longQuantity"] - self.equities["shortQuantity"])
            % 100
            == 0
        ]

    @property
    def equities(self) -> pd.DataFrame:
        return self.positions[self.positions["assetType"] == "EQUITY"]

    @property
    def options(self) -> pd.DataFrame:
        return self.positions[self.positions["assetType"] == "OPTION"]

    @property
    def positions(self) -> pd.DataFrame:
        if self._positions is None:
            return self._empty_postions()

        return self._positions

    @property
    def quotes(self) -> pd.DataFrame:
        if self._quotes is None:
            return self._empty_quotes()

        return self._quotes

    def _get_option_positions(
        self,
        ticker: str,
        contract_type: str,
    ) -> Optional[pd.DataFrame]:
        if ticker not in self.options.index.get_level_values("underlying"):
            return None

        positions = self.options.loc[[ticker]]
        return positions[positions["contract_type"] == contract_type]

    def calls(self, ticker: str) -> Optional[pd.DataFrame]:
        return self._get_option_positions(ticker, "CALL")

    def puts(self, ticker: str) -> Optional[pd.DataFrame]:
        return self._get_option_positions(ticker, "PUT")

    def shares(self, ticker: str) -> Optional[pd.DataFrame]:
        if ticker not in self.equities.index.get_level_values("underlying"):
            return None

        return self.equities.loc[[ticker]]

    async def __call__(self, client: Client, account_id: str):
        positions = await client.positions(account_id)

        if positions is None or positions.empty:
            self._is_loading = False
            return

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

        self._is_loading = False

        self._update()
