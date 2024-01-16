#

import dataclasses
import functools
from typing import Any, Optional

import async_lru as alru
import numpy as np
import pandas as pd
import rich
import rich.console
import rich.progress
import rich.table
from stonkers.client import Client

from .option import Option
from .positions import Positions
from .price import market_price
from .tasks import add_tasks


def count_positions(positions: Optional[pd.DataFrame]) -> int:
    if positions is None or positions.empty:
        return 0

    return np.abs(
        positions["longQuantity"].sum() - positions["shortQuantity"].sum()
    )


def filter_instruction(orders: pd.DataFrame, instruction: str) -> pd.DataFrame:
    if orders.empty:
        return orders

    return orders[
        orders["orderLegCollection"].apply(
            lambda x: len(x) == 1 and x[0]["instruction"] == instruction
        )
    ]


@dataclasses.dataclass
class Ticker:
    # pylint: disable=too-many-public-methods

    account_id: str
    client: Client
    ticker: str

    history_days: int = 35

    positions: Optional[Positions] = None

    def __hash__(self) -> int:
        return hash((self.account_id, self.ticker))

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Ticker):
            return NotImplemented

        return (
            self.account_id == other.account_id and self.ticker == other.ticker
        )

    @property
    @alru.alru_cache(maxsize=1)
    async def history(self) -> pd.DataFrame:
        start_datetime = pd.Timestamp.now() - pd.Timedelta(
            days=self.history_days
        )
        return await self.client.get_price_history_every_day(
            self.ticker, start_datetime
        )

    @property
    @alru.alru_cache(maxsize=1)
    async def quote(self) -> pd.Series:
        return (await self.client.quote(self.ticker)).loc[self.ticker]

    @functools.cached_property
    def calls(self) -> Optional[pd.DataFrame]:
        if self.positions is None:
            return None

        return self.positions.calls(self.ticker)

    @property
    @alru.alru_cache(maxsize=1)
    async def change(self) -> float:
        return np.fabs(await self.price - await self.close)

    @property
    @alru.alru_cache(maxsize=1)
    async def close(self) -> float:
        if await self.client.is_equities_open:
            # If the market is not open, use the close from two days ago
            return (await self.history)["close"].iloc[-2]

        return (await self.quote)["closePrice"]

    @property
    @alru.alru_cache(maxsize=1)
    async def price(self) -> float:
        return market_price(await self.quote)

    @functools.cached_property
    def num_calls(self) -> int:
        return count_positions(self.calls)

    @functools.cached_property
    def num_puts(self) -> int:
        return count_positions(self.puts)

    @functools.cached_property
    def num_shares(self) -> int:
        return count_positions(self.shares)

    @functools.cached_property
    def puts(self) -> Optional[pd.DataFrame]:
        if self.positions is None:
            return None

        return self.positions.puts(self.ticker)

    @functools.cached_property
    def shares(self) -> Optional[pd.DataFrame]:
        if self.positions is None:
            return None

        return self.positions.shares(self.ticker)

    @property
    @alru.alru_cache(maxsize=1)
    async def orders(self) -> pd.DataFrame:
        orders = await self.client.orders(self.account_id)

        return orders[
            orders["orderLegCollection"].apply(
                lambda x: len(x) == 1
                and x[0]["instrument"]["underlyingSymbol"] == self.ticker
            )
        ]

    @property
    @alru.alru_cache(maxsize=1)
    async def had_order_today(self) -> bool:
        orders = pd.concat([await self.open_orders, await self.filled_orders])

        if orders.empty:
            return False

        return orders["enteredTime"].max().date() == pd.Timestamp.now().date()

    @property
    @alru.alru_cache(maxsize=1)
    async def open_orders(self) -> pd.DataFrame:
        orders = await self.orders

        if orders.empty:
            return orders

        return orders[orders["status"].isin(["WORKING", "QUEUED"])]

    @property
    @alru.alru_cache(maxsize=1)
    async def filled_orders(self) -> pd.DataFrame:
        orders = await self.orders

        if orders.empty:
            return orders

        return orders[orders["status"] == "FILLED"]

    @property
    @alru.alru_cache(maxsize=1)
    async def open_sell_to_open_orders(self) -> pd.DataFrame:
        orders = await self.open_orders

        return filter_instruction(
            orders,
            self.client.OrderBuilder.OptionInstruction.SELL_TO_OPEN,
        )

    @property
    @alru.alru_cache(maxsize=1)
    async def open_buy_to_close_orders(self) -> pd.DataFrame:
        orders = await self.open_orders

        return filter_instruction(
            orders,
            self.client.OrderBuilder.OptionInstruction.BUY_TO_CLOSE,
        )

    @property
    @alru.alru_cache(maxsize=1)
    async def filled_sell_to_open_orders(self) -> pd.DataFrame:
        orders = await self.filled_orders

        return filter_instruction(
            orders,
            self.client.OrderBuilder.OptionInstruction.SELL_TO_OPEN,
        )

    @property
    @alru.alru_cache(maxsize=1)
    async def filled_buy_to_close_orders(self) -> pd.DataFrame:
        orders = await self.filled_orders

        return filter_instruction(
            orders,
            self.client.OrderBuilder.OptionInstruction.BUY_TO_CLOSE,
        )

    @alru.alru_cache
    async def existing_order(self, option: Option) -> Optional[pd.Series]:
        orders = await self.open_orders

        if orders.empty:
            return None

        def needle(x: pd.Series) -> bool:
            return (
                x["orderLegCollection"][0]["instrument"]["symbol"]
                == option.symbol
                and x["orderLegCollection"][0]["instruction"]
                == self.client.OrderBuilder.OptionInstruction.SELL_TO_OPEN
                and x["orderLegCollection"][0]["putCall"].lower()
                == option.put_call.lower()
            )

        return orders[orders["orderLegCollection"].apply(needle)].iloc[0]

    async def has_existing_order(self, option: Option) -> bool:
        existing_order = await self.existing_order(option)
        return existing_order is not None and not existing_order.empty

    async def cancel_open_orders(self) -> None:
        orders = await self.open_orders
        if orders.empty:
            return

        await self.client.cancel_order(self.account_id, orders["orderId"])

    async def __call__(
        self,
        progress: rich.progress.Progress,
        task: rich.progress.TaskID,
    ) -> "Ticker":
        add_tasks(progress, task, 3)

        # Preload the history and the quote
        await self.history
        progress.advance(task)

        await self.quote
        progress.advance(task)

        await self.orders
        progress.advance(task)

        return self
