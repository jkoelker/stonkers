#

import functools
import json
import os

import click
import pandas as pd
import yaml

from . import auth, client, commands

APP_NAME = "stonkers"

API_KEY = "api_key"
REDIRECT_URI = "redirect_uri"

OUTPUT_JSON = "json"
OUTPUT_YAML = "yaml"
OUTPUT_CONSOLE = "console"


class Stonkers(object):
    def __init__(self, creds_file, output_format, token_file):
        self.creds_file = creds_file
        self.output_format = output_format
        self.token_file = token_file

    @functools.cached_property
    def client(self):
        with click.open_file(self.creds_file, "r") as f:
            config = yaml.safe_load(f)
            return client.Client(
                auth.get_client(
                    config[API_KEY], config[REDIRECT_URI], self.token_file
                )
            )

    def format(self, data, **kwargs):
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
    stonkers.client


@cli.group()
@click.help_option("-h", "--help")
def account():
    """Account Functions."""
    pass


@account.command()
@click.help_option("-h", "--help")
@click.pass_obj
def list(stonkers):
    """List account IDs."""
    accounts = stonkers.client.accounts()
    print(stonkers.format(accounts.index.to_series(), index=False))


@account.command()
@click.help_option("-h", "--help")
@click.pass_obj
def rebalance(stonkers):
    """Rebalance."""
    pass


@cli.command()
@click.option("-d", "--dte", default=60, help="Days to expiration.")
@click.option(
    "-p", "--pop-min", default=70, help="Probability of Profit minimum."
)
@click.option(
    "-P", "--pop-max", default=90, help="Probability of Profit maximum."
)
@click.option("-r", "--return-min", default=20, help="Retun minimum value.")
@click.argument("tickers", nargs=-1)
@click.help_option("-h", "--help")
@click.pass_obj
def puts(stonkers, dte, pop_min, pop_max, return_min, tickers):
    """Find options that meet an anual rate of return requirement."""
    if not tickers:
        tickers = ("GME",)

    puts = commands.put_finder(
        stonkers.client, tickers, dte, pop_min, pop_max, return_min
    )
    puts = puts[
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

    puts = puts.rename(
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

    print(stonkers.format(puts, index=False))
