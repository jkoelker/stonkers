#

import dataclasses
from typing import Any, Callable, Optional

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

        return self.account[key]

    @property
    def account(self) -> pd.Series:
        if self._account is None:
            raise RuntimeError("Account summary has not been fetched yet.")

        return self._account.loc[self.account_id]

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
