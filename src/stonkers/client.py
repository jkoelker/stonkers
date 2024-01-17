#

import asyncio
import dataclasses
import datetime
import email.utils

import async_lru as alru
import cachetools
import tda.client  # type: ignore
import tenacity
import tenacity.wait

from . import convert
from .orders import OrderBuilder


class RetryHTTPTooManyRequests(tenacity.retry_base):
    # pylint: disable=too-few-public-methods

    def __call__(self, state: tenacity.RetryCallState) -> bool:
        if not state.outcome:
            return False

        exc = state.outcome.exception()
        response = getattr(exc, "response", None)

        if exc is None or response is None:
            return False

        if response.status_code == 429:
            return True

        return False


class WaitRetryAfter(tenacity.wait.wait_base):
    # pylint: disable=too-few-public-methods

    def __call__(self, state: tenacity.RetryCallState) -> float:
        if not state.outcome:
            return 0

        exc = state.outcome.exception()
        response = getattr(exc, "response", None)

        if exc is None or response is None:
            return 0

        after = response.headers.get("retry-after", "0")

        if after.isdigit():
            return int(after)

        try:
            when = email.utils.parsedate_to_datetime(after)
        except (TypeError, ValueError):
            return 0

        return max(0, (when - datetime.datetime.utcnow()).total_seconds())


@dataclasses.dataclass
class Cache:
    orders: cachetools.TTLCache
    options: cachetools.TTLCache
    quotes: cachetools.TTLCache


class Client:
    _cache: Cache

    OrderBuilder = OrderBuilder

    def __init__(self, tda_client: tda.client.AsyncClient):
        self.tda = tda_client

        self._cache = Cache(
            orders=cachetools.TTLCache(maxsize=1000, ttl=10),
            options=cachetools.TTLCache(maxsize=1000, ttl=10),
            quotes=cachetools.TTLCache(maxsize=1000, ttl=10),
        )

    @tenacity.retry(
        reraise=True,
        retry=RetryHTTPTooManyRequests(),
        wait=tenacity.wait_combine(
            WaitRetryAfter(), tenacity.wait_exponential(multiplier=0.2)
        ),
        stop=tenacity.stop_after_attempt(5),
    )
    async def _get(self, func, *args, **kwargs):
        response = await func(*args, **kwargs)
        response.raise_for_status()

        return response

    @staticmethod
    def _accounts(accounts, dataframe=True, principals=None):
        accounts = {
            account["securitiesAccount"]["accountId"]: account[
                "securitiesAccount"
            ]
            for account in accounts
        }

        if principals:
            augment = {
                a["accountId"]: a for a in principals.get("accounts", [])
            }
            for acct in accounts:
                accounts[acct]["displayName"] = augment.get(acct, {}).get(
                    "displayName", ""
                )

        if dataframe:
            return convert.accounts(accounts)

        return accounts

    async def _get_accounts(
        self, account_id=None, fields=None, dataframe=True, augment=True
    ):
        get_func = (
            self.tda.get_account if account_id else self.tda.get_accounts
        )

        accounts = (
            [(await self._get(get_func, account_id, fields=fields)).json()]
            if account_id
            else (await get_func(fields=fields)).json()
        )

        principals = None
        if augment:
            principals = await self.user_principals(dataframe=False)

        return self._accounts(
            accounts, dataframe=dataframe, principals=principals
        )

    @alru.alru_cache
    async def is_equities_open_on(self, date=None) -> bool:
        if date is None:
            date = datetime.date.today()

        result = (
            await self._get(
                self.tda.get_hours_for_multiple_markets,
                markets=self.tda.Markets.EQUITY,
                date=date,
            )
        ).json()

        for key in result:
            for subkey in result.get(key, {}):
                if result[key][subkey].get("marketType") == "EQUITY":
                    return result[key][subkey].get("isOpen", False)

        return False

    @property
    @alru.alru_cache
    async def is_equities_open(self) -> bool:
        return await self.is_equities_open_on()

    def account(self, account_id, fields=None, dataframe=True, augment=True):
        return self._get_accounts(account_id, fields, dataframe, augment)

    def accounts(self, fields=None, dataframe=True, augment=True):
        return self._get_accounts(None, fields, dataframe, augment)

    async def options(self, symbol, dataframe=True, **kwargs):
        if symbol not in self._cache.options:
            self._cache.options[symbol] = (
                await self._get(self.tda.get_option_chain, symbol, **kwargs)
            ).json()

        options = self._cache.options[symbol]

        if dataframe:
            return convert.options(options)

        return options

    async def _quote(self, symbols):
        response = await self._get(self.tda.get_quotes, symbols)
        response.raise_for_status()

        quotes = response.json()

        self._cache.quotes.update(quotes)

        return quotes

    async def quote(self, symbols, dataframe=True):
        if isinstance(symbols, str):
            symbols = [symbols]

        symbols = set(symbols)

        quotes = {}

        for symbol in set(symbols):
            if symbol in self._cache.quotes:
                quotes[symbol] = self._cache.quotes[symbol]
                symbols.remove(symbol)

        if symbols:
            quotes.update(await self._quote(symbols))

        if dataframe:
            return convert.quote(quotes)

        return quotes

    def order_builder(self):
        return self.OrderBuilder()

    async def orders(self, account_id, dataframe=True):
        if account_id not in self._cache.orders:
            account = await self.account(
                account_id,
                fields=self.tda.Account.Fields.ORDERS,
            )

            self._cache.orders[account_id] = account["orderStrategies"].iloc[0]

        data = self._cache.orders[account_id]

        if dataframe:
            return convert.orders(data)

        return data

    async def positions(self, account_id, dataframe=True):
        account = await self.account(
            account_id,
            fields=self.tda.Account.Fields.POSITIONS,
        )

        if "positions" not in account:
            return None

        if dataframe:
            return convert.positions(account["positions"].iloc[0])

        return account["positions"].iloc[0]

    async def user_principals(self, dataframe=True):
        principals = (await self._get(self.tda.get_user_principals)).json()

        if dataframe:
            return convert.user_principals(principals)

        return principals

    async def get_price_history_every_day(
        self,
        symbol,
        start_datetime=None,
        end_datetime=None,
        need_extended_hours_data=None,
        dataframe=True,
    ):
        price_history = (
            await self._get(
                self.tda.get_price_history_every_day,
                symbol,
                end_datetime=end_datetime,
                start_datetime=start_datetime,
                need_extended_hours_data=need_extended_hours_data,
            )
        ).json()

        if dataframe:
            return convert.price_history(price_history)

        return price_history

    async def cancel_order(self, account_id, order_id):
        if isinstance(order_id, str):
            order_id = [order_id]

        if account_id in self._cache.orders:
            del self._cache.orders[account_id]

        for order in order_id:
            await self._get(self.tda.cancel_order, account_id, order)

    async def place_order(self, account_id, order):
        if isinstance(order, (dict, OrderBuilder)):
            order = [order]

        if account_id in self._cache.orders:
            del self._cache.orders[account_id]

        tasks = []
        for _order in order:
            if isinstance(_order, OrderBuilder):
                _order = _order.build()

            tasks.append(self._get(self.tda.place_order, account_id, _order))

        await asyncio.gather(*tasks)
