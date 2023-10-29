#

import datetime as dt

import pandas as pd

from .. import convert


def get_returns(bid, strike_price, dte):
    """Calculate return and annual return for a sold option."""
    put_return = bid / (strike_price - bid) * 100
    annual_return = put_return / dte.apply(lambda x: x if x > 0 else 1) * 365
    return (round(put_return, 1), round(annual_return, 1))


async def put_finder(
    client, tickers, dte=60, pop_min=70, pop_max=90, return_min=20
):
    # Set the max DTE for options chains.
    max_exp = dt.datetime.now() + dt.timedelta(days=dte)

    # Get the options chain as a pandas dataframe. (Thanks dobby. 🤗)
    chains = []
    for ticker in tickers:
        chain = await client.options(
            ticker,
            contract_type=client.tda.Options.ContractType.PUT,
            include_quotes=True,
            to_date=max_exp,
            option_type=client.tda.Options.Type.STANDARD,
            dataframe=False,
        )

        underlying = chain.get("underlying")

        if chain:
            chain = convert.options(chain)

            if underlying:
                for key, value in underlying.items():
                    chain[f"underlying.{key}"] = value

                chains.append(chain)

    options = pd.concat(chains)

    if options.empty:
        return options

    # Calculate a return for the trade and an annualized return.
    options["putReturn"], options["annualReturn"] = get_returns(
        options["bid"], options["strikePrice"], options["daysToExpiration"]
    )

    # Calculate PoP based on the delta.
    options["pop"] = (1 - options["delta"].abs()) * 100

    # Remove the time of day information from the expiration date.
    options["expirationDate"] = options["expirationDate"].dt.strftime(
        "%Y-%m-%d"
    )

    # Select options that meet all of our requirements.
    return options[
        (options["annualReturn"] >= return_min)
        & (pop_min <= options["pop"])
        & (options["pop"] <= pop_max)
    ].sort_values("annualReturn", ascending=False)
