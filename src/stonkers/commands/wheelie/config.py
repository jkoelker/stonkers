#

import dataclasses
import functools
from typing import Any, Callable, Dict, List, get_type_hints

import pandas as pd

from . import conditions


@dataclasses.dataclass
class WheelConfig:
    # NOTE this docstring is inserted into cli help, so it should be formatted
    #      accordingly
    """
    \b
    - max_contracts_percent:
        The maximum percent of the portfolio's buying power to buy
        contracts for a single day. (default: 0.05 or 5%)

    \b
    - min_contract_price:
        The minimum price of a contract to consider. Should be higher
        than the round trip commission for the broker.
        (default: 0.05 or $5/contract)

    \b
    - sigma:
        The threshold percent of standard deviation before selling.
        For example, if the standard deviation is 0.2 and the sigma
        is 0.1, the option will be sold when the price moves 0.02
        (default: 0.1 or 10%)

    \b
    - std_dev_window:
        The number of days to calculate the standard deviation
        (default: 35 days)

    \b
    - weight:
        The weight of the ticker in the portfolio (default: 0.2 or 20%)
    """

    ticker: str
    max_contracts_percent: float = 0.05  # 5%
    min_contract_price: float = 0.05  # $0.05 or $5/contract
    sigma: float = 0.1  # 10% std dev
    std_dev_window: int = 35  # 35 days
    weight: float = 0.2  # 20%

    def __hash__(self) -> int:
        return hash(
            (
                self.ticker,
                self.max_contracts_percent,
                self.sigma,
                self.std_dev_window,
                self.weight,
            )
        )

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, WheelConfig):
            return NotImplemented

        return (
            self.ticker == other.ticker
            and self.max_contracts_percent == other.max_contracts_percent
            and self.sigma == other.sigma
            and self.std_dev_window == other.std_dev_window
            and self.weight == other.weight
        )

    @functools.cached_property
    def conditions(self) -> List[Callable[[pd.DataFrame], pd.Series]]:
        return [
            conditions.days_to_expiration(min_days=7, max_days=65),
            conditions.exclude_in_the_money(),
            conditions.delta(),
        ]

    @functools.cached_property
    def call_conditions(self) -> List[Callable[[pd.DataFrame], pd.Series]]:
        return self.conditions + [
            conditions.is_call(),
        ]

    @functools.cached_property
    def put_conditions(self) -> List[Callable[[pd.DataFrame], pd.Series]]:
        return self.conditions + [
            conditions.is_put(),
        ]

    @classmethod
    def parse(cls, ticker: str) -> "WheelConfig":
        options = ticker.split(":")
        symbol = options[0]
        kwargs: Dict[str, Any] = {"ticker": symbol}

        field_types = get_type_hints(cls)

        for option in options[1:]:
            key, value = option.split("=")

            if key not in field_types:
                raise ValueError(f"Unknown option: {key}")

            expected_type = field_types[key]
            try:
                # Use the type hint to cast the value to the correct type
                kwargs[key] = expected_type(value)
            except ValueError as e:
                raise ValueError(
                    f"Could not convert value for field '{key}': {value}"
                ) from e

        return cls(**kwargs)
