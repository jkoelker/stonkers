#

from . import convert


class Client(object):
    def __init__(self, tda_client):
        self.tda = tda_client

    @staticmethod
    def _accounts(accounts, dataframe=True):
        accounts = {a["securitiesAccount"]["accountId"]: a for a in accounts}

        if dataframe:
            return convert.accounts(accounts)

        return accounts

    def account(self, account_id, fields=None, dataframe=True):
        accounts = self.tda.get_account(account_id, fields=fields).json()
        return self._accounts([accounts], dataframe=dataframe)

    def accounts(self, fields=None, dataframe=True):
        accounts = self.tda.get_accounts(fields=fields).json()
        return self._accounts(accounts, dataframe=dataframe)

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
