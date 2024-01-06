#

import asyncio
import dataclasses
import functools
from typing import Any, Callable, List, Optional

import async_lru as alru
import numpy as np
import rich
import rich.console
import rich.progress
import rich.table
from stonkers.client import Client

from . import conditions
from .account import AccountSummary
from .config import WheelConfig
from .formatting import colorize, join, number
from .option import Option
from .positions import Positions
from .tasks import add_tasks
from .ticker import Ticker


@dataclasses.dataclass
class Wheel:
    # pylint: disable=too-many-instance-attributes
    # pylint: disable=too-many-public-methods

    account_id: str

    config: WheelConfig

    account_summary: AccountSummary
    client: Client
    positions: Positions

    _display: Optional[List[rich.console.RenderableType]] = None

    options: List[Option] = dataclasses.field(default_factory=list)

    def __post_init__(self) -> None:
        self._ticker = Ticker(
            self.account_id,
            self.client,
            self.ticker,
            history_days=self.config.std_dev_window,
            positions=self.positions,
        )

    def __hash__(self) -> int:
        return hash((self.account_id, self.config))

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Wheel):
            return NotImplemented

        return (
            self.account_id == other.account_id and self.config == other.config
        )

    def __rich__(self) -> rich.console.RenderableType:
        if self._display is not None:
            return rich.console.Group(*self._display)

        return "Loading..."

    def __display(self, *args: rich.console.RenderableType) -> None:
        if self._display is None:
            self._display = []

        self._display.extend(args)

    def add_option(self, *options: Option) -> None:
        self.options.extend(options)

    @functools.cached_property
    def ticker(self) -> str:
        return self.config.ticker

    @property
    @alru.alru_cache(maxsize=1)
    async def is_red(self) -> bool:
        return await self._ticker.price < await self._ticker.close

    @property
    @alru.alru_cache(maxsize=1)
    async def is_green(self) -> bool:
        return await self._ticker.price > await self._ticker.close

    @functools.cached_property
    def excess_calls(self) -> int:
        if self.has_excess_calls:
            return np.abs(self.net_target_calls)

        return 0

    @property
    @alru.alru_cache(maxsize=1)
    async def excess_puts(self) -> int:
        if await self.has_excess_puts:
            return np.abs(await self.net_target_puts)

        return 0

    @functools.cached_property
    def has_excess_calls(self) -> bool:
        return self.net_target_calls < 0

    @property
    @alru.alru_cache(maxsize=1)
    async def has_excess_puts(self) -> bool:
        return await self.net_target_puts < 0

    @property
    @alru.alru_cache(maxsize=1)
    async def maximum_new_contracts(self) -> int:
        buying_power = (
            self.target_buying_power * self.config.max_contracts_percent
        )
        return max(
            [1, round((buying_power / (await self._ticker.price)) // 100)]
        )

    @functools.cached_property
    def net_target_calls(self) -> int:
        return int(
            np.floor(self._ticker.num_shares / 100) - self._ticker.num_calls
        )

    @property
    @alru.alru_cache(maxsize=1)
    async def net_target_puts(self) -> int:
        return int(
            np.floor(await self.target_shares / 100) - self._ticker.num_puts
        )

    @property
    @alru.alru_cache(maxsize=1)
    async def net_target_shares(self) -> int:
        return int(
            np.floor(
                (await self.target_shares)
                - self._ticker.num_shares
                - (self._ticker.num_puts * 100),
            )
        )

    @property
    def has_shares(self) -> bool:
        return self._ticker.num_shares > 0

    @property
    @alru.alru_cache(maxsize=1)
    async def excess_shares(self) -> int:
        return np.abs(
            int(np.floor((await self.target_shares) - self._ticker.num_shares))
        )

    @property
    @alru.alru_cache(maxsize=1)
    async def has_excess_shares(self) -> bool:
        if not self.has_shares:
            return False

        return await self.net_target_shares < 0

    @functools.cached_property
    def target_buying_power(self) -> float:
        return self.account_summary.target_buying_power * self.config.weight

    @property
    @alru.alru_cache(maxsize=1)
    async def target_shares(self) -> int:
        # Round down to the nearest 100
        return int(
            np.floor(
                self.target_buying_power / (await self._ticker.price) / 100
            )
            * 100
        )

    @property
    @alru.alru_cache(maxsize=1)
    async def to_write_calls(self) -> int:
        if self.net_target_calls < 0:
            return 0

        return min([await self.maximum_new_contracts, self.net_target_calls])

    @property
    @alru.alru_cache(maxsize=1)
    async def to_write_puts(self) -> int:
        if await self.net_target_puts < 0:
            return 0

        return min(
            [await self.maximum_new_contracts, await self.net_target_puts]
        )

    @property
    @alru.alru_cache(maxsize=1)
    async def write_threshold(self) -> float:
        return (
            await self._ticker.close
            * (np.exp(np.log((await self._ticker.history)["close"]).std()) - 1)
            * self.config.sigma
        )

    def __display_excess(self, num_contracts: int, put_call: str) -> None:
        excess = colorize(f"{num_contracts}", "yellow1")

        self.__display(
            f"[yellow]Warning: excess {put_call} ({excess})[/yellow]"
        )

    async def __display_target(self, num_contracts, put_call: str) -> None:
        target_display = join(
            f"Writing {number(num_contracts, precision=0 )} {put_call},",
            f"target: {number(self.net_target_calls, precision=0)}",
            f"max per day: {number(await self.maximum_new_contracts, precision=0)}",
        )

        self.__display(colorize(target_display, "green"))

    def __display_skipping(self, put_call: str, why: str) -> None:
        self.__display(f"[cyan]Skipping writting {put_call}: {why}[/cyan]")

    async def write_calls(
        self,
        progress: rich.progress.Progress,
        task: rich.progress.TaskID,
    ):
        add_tasks(progress, task, 5)

        if self.has_excess_calls:
            self.__display_excess(self.excess_calls, "calls")

            return self

        progress.advance(task)

        if await self.to_write_calls == 0:
            self.__display_skipping("calls", "no calls to write")

            return self

        progress.advance(task)

        if not await self.is_green:
            to_write_calls = colorize(f"{await self.to_write_calls}", "cyan1")
            self.__display_skipping(
                f"{to_write_calls} calls",
                "underlying is not green",
            )

            return self

        progress.advance(task)

        if await self._ticker.change < await self.write_threshold:
            change = colorize(f"{(await self._ticker.change):.2f}", "cyan1")
            threashold = colorize(
                f"{(await self.write_threshold):.2f}", "cyan1"
            )

            self.__display_skipping(
                f"{self.net_target_calls} calls",
                f"change ({change}) is less than threshold ({threashold})",
            )

            return self

        progress.advance(task)

        if await self._ticker.had_order_today:
            self.__display_skipping("calls", "already wrote an option today")

            return self

        num_contracts = await self.to_write_calls
        await self.__display_target(num_contracts, "calls")

        option = conditions.best(
            await self.client.options(self.ticker),
            filter_conditions=self.config.call_conditions,
        )

        if option is None or option.empty:
            self.__display_skipping("calls", "no options meet conditions")

            return self

        progress.advance(task)

        to_write = Option(option, self.ticker, num_contracts)
        self.add_option(to_write)

        self.__display(to_write)

    async def write_puts(
        self,
        progress: rich.progress.Progress,
        task: rich.progress.TaskID,
    ):
        # pylint: disable=too-many-return-statements

        add_tasks(progress, task, 6)

        if await self.has_excess_shares:
            self.__display_excess(await self.excess_shares, "shares")

            return self

        progress.advance(task)

        if await self.has_excess_puts:
            self.__display_excess(await self.excess_puts, "puts")

            return self

        progress.advance(task)

        if await self.net_target_shares < 0:
            net_target_shares = number(await self.net_target_shares)

            self.__display_skipping(
                "puts",
                f"net target shares is negative ({net_target_shares})",
            )

            return self

        if not await self.is_red:
            to_write_puts = colorize(f"{await self.to_write_puts}", "cyan1")

            self.__display_skipping(
                f"{to_write_puts} puts", "underlying is not red"
            )

            return self

        progress.advance(task)

        if await self._ticker.change < await self.write_threshold:
            change = colorize(f"{(await self._ticker.change):.2f}", "cyan1")
            threashold = colorize(
                f"{(await self.write_threshold):.2f}", "cyan1"
            )

            self.__display_skipping(
                f"{await self.net_target_puts} puts",
                f"change ({change}) is less than threshold ({threashold})",
            )

            return self

        progress.advance(task)

        if await self.to_write_puts == 0:
            target_puts = colorize(f"{await self.net_target_puts}", "cyan1")

            self.__display_skipping(f"{target_puts} puts", "no puts to write")

            return self

        progress.advance(task)

        if await self._ticker.had_order_today:
            self.__display_skipping("puts", "already wrote an option today")

            return self

        num_contracts = await self.to_write_puts
        await self.__display_target(num_contracts, "puts")

        option = conditions.best(
            await self.client.options(self.ticker),
            filter_conditions=self.config.put_conditions,
        )

        if option is None or option.empty:
            self.__display_skipping("puts", "no options meet conditions")

            return self

        progress.advance(task)

        to_write = Option(option, self.ticker, num_contracts)
        self.add_option(to_write)

        self.__display(to_write)

    async def __call__(
        self,
        progress: rich.progress.Progress,
        done: Optional[Callable[[], None]] = None,
    ) -> "Wheel":
        try:
            task = progress.add_task(
                f"Calculating wheel for {self.ticker}...",
            )

            if (await self.client.options(self.ticker)).empty:
                self.__display(
                    f"[cyan]Skipping {self.ticker}: no options[/cyan]"
                )

                return self

            await self._ticker(progress, task)

            await asyncio.gather(
                self.write_puts(progress, task),
                self.write_calls(progress, task),
            )

            return self

        finally:
            progress.update(task, completed=progress.tasks[task].total)

            if done is not None:
                done()
