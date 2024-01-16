#

import pandas as pd


def market_price(quote: pd.Series) -> float:
    bid = "bidPrice"
    ask = "askPrice"
    last = "lastPrice"

    if "bid" in quote:
        bid = "bid"

    if "ask" in quote:
        ask = "ask"

    if "last" in quote:
        last = "last"

    # if the last price is between the current bid and ask, use the last price
    if quote[bid] < quote[last] < quote[ask]:
        return quote[last]

    return quote["mark"]
