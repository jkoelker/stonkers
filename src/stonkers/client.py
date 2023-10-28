#

from . import convert


class Client():
    def __init__(self, tda_client):
        self.tda = tda_client

    @staticmethod
    def _accounts(accounts, dataframe=True, principals=None):
        accounts = {
            a["securitiesAccount"]["accountId"]: a["securitiesAccount"]
            for a in accounts
        }

        if principals:
            augment = {
                a["accountId"]: a for a in principals.get("accounts", [])
            }

            for account in accounts:
                accounts[account]["displayName"] = augment.get(
                    account, {}
                ).get("displayName", "")

        if dataframe:
            return convert.accounts(accounts)

        return accounts

    def account(self, account_id, fields=None, dataframe=True, augment=True):
        accounts = self.tda.get_account(account_id, fields=fields).json()
        principals = None

        if augment:
            principals = self.user_principals(dataframe=False)

        return self._accounts(
            [accounts], dataframe=dataframe, principals=principals
        )

    def accounts(self, fields=None, dataframe=True, augment=True):
        accounts = self.tda.get_accounts(fields=fields).json()
        principals = None

        if augment:
            principals = self.user_principals(dataframe=False)

        return self._accounts(
            accounts, dataframe=dataframe, principals=principals
        )

    def options(self, symbol, dataframe=True, **kwargs):
        options = self.tda.get_option_chain(symbol, **kwargs).json()

        if dataframe:
            return convert.options(options)

        return options

    def quote(self, symbols, dataframe=True):
        quotes = self.tda.get_quotes(symbols).json()

        if dataframe:
            return convert.quote(quotes)

        return quotes

    def positions(self, account_id, dataframe=True):
        account = self.account(
            account_id, fields=self.tda.Account.Fields.POSITIONS
        )
        positions = account["positions"].iloc[0]

        if dataframe:
            return convert.positions(positions)

        return positions

    def user_principals(self, dataframe=True):
        principals = self.tda.get_user_principals().json()

        if dataframe:
            return convert.user_principals(principals)

        return principals
