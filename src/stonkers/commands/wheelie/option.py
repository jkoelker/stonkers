#

import dataclasses
import functools

import pandas as pd
import rich
import rich.console
import rich.progress
import rich.table
from stonkers import orders

from .formatting import colorize, join, number
from .price import market_price


@dataclasses.dataclass
class Option:
    data: pd.Series
    ticker: str
    num_contracts: int = 0

    @functools.cached_property
    def delta(self) -> float:
        return self.data["delta"]

    @functools.cached_property
    def expiration_date(self) -> str:
        if "expirationDate" not in self.data:
            return ""

        date = self.data["expirationDate"]
        if not isinstance(date, pd.Timestamp):
            date = pd.Timestamp(date)

        return date.strftime("%b %d %Y")

    @functools.cached_property
    def is_call(self) -> bool:
        return self.data["putCall"].lower() == "call"

    @functools.cached_property
    def is_put(self) -> bool:
        return self.data["putCall"].lower() == "put"

    @functools.cached_property
    def multiplier(self) -> int:
        return self.data["multiplier"]

    @functools.cached_property
    def return_on_risk(self) -> float:
        return self.data["RoR"]

    @functools.cached_property
    def strike_price(self) -> float:
        return self.data["strikePrice"]

    @functools.cached_property
    def price(self) -> float:
        return market_price(self.data)

    @functools.cached_property
    def put_call(self) -> str:
        return self.data["putCall"].title()

    @functools.cached_property
    def symbol(self) -> str:
        return self.data["symbol"]

    @functools.cached_property
    def underlying(self) -> str:
        if "underlying" in self.data:
            return self.data["underlying"]

        return self.ticker

    def __rich__(self) -> rich.console.RenderableType:
        if self.num_contracts == 0:
            return f"[cyan]Skipping {self.ticker}: no options meet conditions.[/cyan]"

        put_call = colorize(f"{self.put_call}", "orange3")
        if self.is_put:
            put_call = colorize(f"{self.put_call}", "deep_pink3")

        count = colorize(f"{self.num_contracts}", "chartreuse1")
        ticker = colorize(f"{self.ticker}", "cyan1")
        date = colorize(f"{self.expiration_date}", "khaki3")
        strike = number(self.strike_price)
        price = number(self.price, currency="$")
        delta = colorize(f"Δ {self.delta:.4f}", "chartreuse1")
        total = number(
            self.num_contracts * self.price * self.multiplier, currency="$"
        )
        ror = number(self.return_on_risk, percent=True)

        msg = join(
            f"Write {count} x {ticker} {date} {strike} {put_call} {delta}",
            f"for {price} for a total of {total} with a RoR of {ror}.",
        )

        return colorize(msg, "green")

    def sell_order(self) -> orders.OrderBuilder:
        return orders.OrderBuilder.option_sell_to_open_limit(
            symbol=self.symbol,
            quantity=self.num_contracts,
            price=self.price,
        )

    def buy_order(self, profit_target=0.5) -> orders.OrderBuilder:
        return orders.OrderBuilder.option_buy_to_close_limit(
            symbol=self.symbol,
            quantity=self.num_contracts,
            price=self.price * (1 - profit_target),
        ).set_duration(orders.OrderBuilder.Duration.GOOD_TILL_CANCEL)

    def trigger_order(self, profit_target=0.5) -> orders.OrderBuilder:
        return (
            self.sell_order()
            .set_order_strategy_type(
                orders.OrderBuilder.OrderStrategyType.TRIGGER
            )
            .add_child_order_strategy(
                self.buy_order(profit_target=profit_target)
            )
        )
