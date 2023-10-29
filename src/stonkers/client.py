#

import tda.client

from . import convert


class Client:
    def __init__(self, tda_client: tda.client.AsyncClient):
        self.tda = tda_client

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
            [(await get_func(account_id, fields=fields)).json()]
            if account_id
            else (await get_func(fields=fields)).json()
        )

        principals = None
        if augment:
            principals = await self.user_principals(dataframe=False)

        return self._accounts(
            accounts, dataframe=dataframe, principals=principals
        )

    def account(self, account_id, fields=None, dataframe=True, augment=True):
        return self._get_accounts(account_id, fields, dataframe, augment)

    def accounts(self, fields=None, dataframe=True, augment=True):
        return self._get_accounts(None, fields, dataframe, augment)

    async def options(self, symbol, dataframe=True, **kwargs):
        options = (await self.tda.get_option_chain(symbol, **kwargs)).json()

        if dataframe:
            return convert.options(options)

        return options

    async def quote(self, symbols, dataframe=True):
        quotes = (await self.tda.get_quotes(symbols)).json()

        if dataframe:
            return convert.quote(quotes)

        return quotes

    async def positions(self, account_id, dataframe=True):
        account = await self.account(
            account_id,
            fields=self.tda.Account.Fields.POSITIONS,
        )
        positions = account["positions"].iloc[0]

        if dataframe:
            return convert.positions(positions)

        return positions

    async def user_principals(self, dataframe=True):
        principals = (await self.tda.get_user_principals()).json()

        if dataframe:
            return convert.user_principals(principals)

        return principals
