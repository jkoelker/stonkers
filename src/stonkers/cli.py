#
"""
Stonkers CLI.
"""

import asyncio
import functools
import inspect
import json
import os
import urllib.parse

import click
import httpx
import pandas as pd
import yaml

from . import auth, client, commands

APP_NAME = "stonkers"

API_KEY = "api_key"
REDIRECT_URI = "redirect_uri"

OUTPUT_JSON = "json"
OUTPUT_YAML = "yaml"
OUTPUT_CONSOLE = "console"


def make_sync(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return asyncio.run(func(*args, **kwargs))

    return wrapper


class ThetaGang:  # pylint:disable=too-few-public-methods
    """
    ThetaGang is a wrapper around the ThetaGang API.
    """

    HOST = "https://api.thetagang.com"

    def trending(self):
        """Get trending tickers from ThetaGang."""
        url = urllib.parse.urljoin(self.HOST, "/trends")
        request = httpx.get(url, timeout=5)

        try:
            request.raise_for_status()
        except httpx.HTTPError:
            return []

        return request.json().get("data", {}).get("trends", [])


class Stonkers:
    """
    Stonkers is the main object for the CLI.
    """

    def __init__(self, creds_file, output_format, token_file):
        self.creds_file = creds_file
        self.output_format = output_format
        self.token_file = token_file

    @functools.cached_property
    def thetagang(self):
        """ThetaGang API."""
        return ThetaGang()

    def default_tickers(self, exclude):
        tickers = self.thetagang.trending()

        if not tickers:
            tickers = ({"symbol": "GME"},)

        return [
            t["symbol"]
            for t in tickers
            if t.get("symbol") not in exclude + (None,)
        ]

    @functools.cached_property
    def client(self):
        """TD Ameritrade Client."""
        with click.open_file(self.creds_file, "r") as file:
            config = yaml.safe_load(file)
            return client.Client(
                auth.get_client(
                    config[API_KEY],
                    config[REDIRECT_URI],
                    self.token_file,
                    asyncio=True,
                )
            )

    def format(self, data, **kwargs):
        """Format the data based on the output format."""
        if self.output_format == OUTPUT_JSON:
            if hasattr(data, "to_json"):
                return data.to_json(orient="records", indent=2)

            return json.dumps(data, indent=2)

        if self.output_format == OUTPUT_YAML:
            if isinstance(data, (pd.Index, pd.Series)):
                data = data.to_list()

            if isinstance(data, pd.DataFrame):
                data = data.to_dict(orient="records")

            return yaml.safe_dump(data)

        if hasattr(data, "to_markdown"):
            return data.to_markdown(**kwargs)

        return yaml.dump(data)


click.option = functools.partial(click.option, show_default=True)


class AsyncContext(click.Context):
    def invoke(self, *args, **kwargs):  # pylint: disable=arguments-differ
        r = super().invoke(*args, **kwargs)

        if inspect.isawaitable(r):
            return asyncio.run(r)

        return r


click.Command.context_class = AsyncContext


@click.group()
@click.option(
    "-c",
    "--creds-file",
    envvar="CREDS_FILE",
    type=click.Path(allow_dash=True),
    help="Credentials yaml file containing `api_key` and `redirect_uri`.",
    default=os.path.join(click.get_app_dir(APP_NAME), "creds.yaml"),
)
@click.option(
    "-o",
    "--output",
    type=click.Choice(
        [OUTPUT_JSON, OUTPUT_YAML, OUTPUT_CONSOLE], case_sensitive=False
    ),
    help="Output format",
    default=OUTPUT_CONSOLE,
)
@click.option(
    "-t",
    "--token-file",
    envvar="TOKEN_FILE",
    type=click.Path(),
    help="Token file for TD Ameritrade OAuth.",
    default=os.path.join(click.get_app_dir(APP_NAME), "token.json"),
)
@click.version_option(None, "-V", "--version")
@click.help_option("-h", "--help")
@click.pass_context
def cli(ctx, creds_file, output, token_file):
    """Main CLI Entrypoint"""
    ctx.obj = Stonkers(creds_file, output, token_file)


@cli.command()
@click.option(
    "-a",
    "--api-key",
    prompt=True,
    hide_input=True,
    confirmation_prompt=True,
    envvar="API_KEY",
)
@click.option("-r", "--redirect-uri", prompt=True, envvar="REDIRECT_URI")
@click.help_option("-h", "--help")
@click.pass_obj
def setup(stonkers, api_key, redirect_uri):
    """Setup TD Ameritrade client and perform initial OAuth."""

    os.makedirs(click.get_app_dir(APP_NAME), mode=0o700, exist_ok=True)

    if stonkers.creds_file != "-":
        with click.open_file(stonkers.creds_file, "w") as f:
            yaml.dump({API_KEY: api_key, REDIRECT_URI: redirect_uri}, f)

        os.chmod(stonkers.creds_file, 0o600)

    # NOTE(jkoelker) Access the property to kick off the login flow
    if stonkers.client is not None:
        print("Setup complete")


@cli.group()
@click.help_option("-h", "--help")
def account():
    """Account Functions."""


@account.command(name="list")
@click.help_option("-h", "--help")
@click.pass_obj
async def list_accounts(stonkers):
    """List account IDs."""
    accounts = await stonkers.client.accounts()
    accounts = accounts[["displayName", "currentBalances.moneyMarketFund"]]

    accounts = accounts.rename(
        columns={
            "displayName": "Name",
            "currentBalances.moneyMarketFund": "Money Market",
        }
    )

    print(stonkers.format(accounts))


@account.command()
@click.help_option("-h", "--help")
@click.argument("account_id")
@click.argument("funds", type=float)
@click.option("--risk", "-r", default=90)
@click.pass_obj
async def rebalance(stonkers, account_id, funds, risk):
    """Rebalance."""
    # TODO(jkoelker) make this configurable
    # 90% stock 10% bonds portfolio
    # bonds: 80% domestic 20% international
    # stock: 60% domestic 30% international 5% reit 5% tech
    bonds = 100 - risk
    allocations = pd.Series(
        {
            "VGT": risk * 0.05 / 100,  # Vanguard Information Technology ETF
            "VNQ": risk * 0.05 / 100,  # Vanguard Real Estate ETF
            "VTI": risk * 0.6 / 100,  # Vanguard Total Stock Market ETF
            "VXUS": risk * 0.3 / 100,  # Vanguard Total International Stock ETF
            "BND": bonds * 0.8 / 100,  # Vanguard Total Bond Market ETF
            "BNDX": bonds * 0.2 / 100,  # Vanguard Total International Bond ETF
        }
    )

    positions = await stonkers.client.positions(account_id)
    portfolio = positions["longQuantity"].reindex(allocations.keys()).fillna(0)
    prices = (await stonkers.client.quote(portfolio.keys()))["askPrice"]

    shares = commands.rebalance(allocations, funds, portfolio, prices)

    cost = shares * prices
    buy = pd.DataFrame({"Shares": shares, "Cost": cost})
    buy.index.name = "Symbol"
    buy.loc["Total"] = pd.Series(buy["Cost"].sum(), index=("Cost",))
    buy["Cost"] = buy["Cost"].apply(lambda x: f"${x:,.2f}")

    def display_portfolio(portfolio, prices, allocations):
        values = portfolio * prices
        value = sum(values)
        balance = portfolio * prices / value
        drift = balance - allocations

        summary = pd.DataFrame(
            {
                "Shares": portfolio,
                "Value": values,
                "Price": prices,
                "Balance": balance,
                "Allocation": allocations,
                "Drift": drift,
            }
        )
        summary.index.name = "Symbol"
        summary.loc["Total"] = pd.Series(
            summary["Value"].sum(), index=("Value",)
        )

        number_fields = ["Shares", "Value", "Price"]
        percentage_fields = ["Balance", "Allocation", "Drift"]

        summary[number_fields] = summary[number_fields].map(
            lambda x: f"{x:,.2f}"
        )
        summary[percentage_fields] = summary[percentage_fields].map(
            lambda x: f"{x:,.2%}"
        )

        return summary

    print("Current Portfolio:")
    current = display_portfolio(portfolio, prices, allocations)
    print(stonkers.format(current))
    print()

    print(
        f"Buy for a total cost of ${sum(cost):,.2f} (${funds - sum(cost):,.2f} left)"
    )
    print(stonkers.format(buy))
    print()

    print("Result Portfolio:")
    result = display_portfolio(portfolio + shares, prices, allocations)
    print(stonkers.format(result))


@cli.group(name="options")
@click.help_option("-h", "--help")
def options_group():
    """Options Functions."""


@options_group.command(name="expiring")
@click.option("-d", "--dte", default=5, help="Days to expiration.")
@click.argument("account_id")
@click.help_option("-h", "--help")
@click.pass_obj
async def expiring_options(stonkers, dte, account_id):
    """Find option positions that are expiring within DTE."""
    positions = await stonkers.client.positions(account_id)

    options = positions[
        positions["assetType"] == stonkers.client.tda.Markets.OPTION.value
    ]

    prices = await stonkers.client.quote(options.index)

    # NOTE(jkoelker) Make a new DataFrame so it is is not a view
    expiring = pd.DataFrame(prices[prices["daysToExpiration"] <= dte])

    if expiring.empty:
        print(f"Nothing expiring in {dte} days")
        return

    expiring["quantity"] = options["longQuantity"] - options["shortQuantity"]
    expiring["premium"] = expiring["quantity"] * options["averagePrice"]

    expiring["profitLoss"] = (
        expiring["quantity"] * expiring["bidPrice"]
    ).where(
        cond=(expiring["quantity"] > 0),
        other=expiring["quantity"] * expiring["askPrice"],
    ) - expiring[
        "premium"
    ]

    expiring["expirationDate"] = pd.to_datetime(
        expiring[
            ["expirationYear", "expirationMonth", "expirationDay"]
        ].rename(
            columns={
                "expirationYear": "year",
                "expirationMonth": "month",
                "expirationDay": "day",
            }
        )
    )
    expiring["expirationDate"] = expiring["expirationDate"].dt.date

    expiring.reset_index(level=0, inplace=True)

    expiring = expiring[
        [
            "symbol",
            "quantity",
            "underlyingPrice",
            "strikePrice",
            "expirationDate",
            "daysToExpiration",
            "bidPrice",
            "askPrice",
            "premium",
            "profitLoss",
        ]
    ]

    expiring = expiring.rename(
        columns={
            "symbol": "💸",
            "quantity": "Quantity",
            "underlyingPrice": "Underlying",
            "strikePrice": "Strike",
            "expirationDate": "Exp Date",
            "daysToExpiration": "DTE",
            "bidPrice": "Bid",
            "askPrice": "Ask",
            "premium": "Premium",
            "profitLoss": "P/L Open",
        }
    )

    print(stonkers.format(expiring, index=False))


@options_group.command()
@click.option("-d", "--dte", default=60, help="Days to expiration.")
@click.option(
    "-p", "--pop-min", default=70, help="Probability of Profit minimum."
)
@click.option(
    "-P", "--pop-max", default=90, help="Probability of Profit maximum."
)
@click.option("-r", "--return-min", default=20, help="Retun minimum value.")
@click.option(
    "-e", "--exclude", multiple=True, help="Exclude a ticker explicitly."
)
@click.argument("tickers", nargs=-1)
@click.help_option("-h", "--help")
@click.pass_obj
# pylint: disable=too-many-arguments
async def puts(stonkers, dte, pop_min, pop_max, return_min, exclude, tickers):
    """Find options that meet an anual rate of return requirement."""
    if not tickers:
        tickers = stonkers.default_tickers(exclude)

    options = await commands.put_finder(
        stonkers.client, tickers, dte, pop_min, pop_max, return_min
    )
    options = options[
        [
            "symbol",
            "underlying.last",
            "strikePrice",
            "expirationDate",
            "daysToExpiration",
            "bid",
            "pop",
            "putReturn",
            "annualReturn",
        ]
    ]

    options = options.rename(
        columns={
            "symbol": "💸",
            "underlying.last": "Underlying",
            "strikePrice": "Strike",
            "expirationDate": "Exp Date",
            "daysToExpiration": "DTE",
            "bid": "Bid",
            "pop": "PoP %",
            "putReturn": "Ret. %",
            "annualReturn": "Annual %",
        }
    )

    print(stonkers.format(options, index=False))


@options_group.command()
@click.option(
    "-e", "--exclude", multiple=True, help="Exclude a ticker explicitly."
)
@click.argument("account_id")
@click.argument("tickers", nargs=-1)
@click.help_option("-h", "--help")
@click.pass_obj
# pylint: disable=too-many-arguments
async def wheelie(stonkers, exclude, account_id, tickers):
    """Run the wheel strategy on the tickers"""
    if not tickers:
        tickers = stonkers.default_tickers(exclude)

    await commands.wheelie(stonkers.client, account_id, tickers)
