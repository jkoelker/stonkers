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
    client,
    tickers,
    dte_min=0,
    dte_max=60,
    pop_min=70,
    pop_max=90,
    return_min=20,
):
    """Find put options that meet our criteria."""
    now = dt.datetime.now().date()

    # Get the options chain as a pandas dataframe. (Thanks dobby. 🤗)
    chains = []
    for ticker in tickers:
        chain = await client.options(
            ticker,
            contract_type=client.tda.Options.ContractType.PUT,
            include_quotes=True,
            from_date=now + dt.timedelta(days=dte_min - 1),
            to_date=now + dt.timedelta(days=dte_max + 1),
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
