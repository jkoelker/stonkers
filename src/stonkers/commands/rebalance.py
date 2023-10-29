#

import numpy as np
from scipy import optimize as opt


def rebalance(allocations, funds, portfolio, prices):
    values = portfolio * prices

    shares = allocations * funds / prices
    shares.update(np.ceil(shares[shares < 1]))
    shares = np.floor(shares).astype(int)

    if sum(values) > 0:

        def optimize(s):
            values = portfolio.add(s, fill_value=0) * prices
            balance = values / sum(values)

            if any(b < 0 for b in balance[s > 0]):
                return 1

            return sum(allocations - balance)

        def funds_constraint(s):
            return funds - sum(s * prices)

        # NOTE(jkoelker) figure out what this does exactly again ;(
        def minimize_constraint(x):
            return min(x[i] - int(x[i]) for i in range(len(x)))

        constraints = (
            {"type": "eq", "fun": funds_constraint},
            {"type": "eq", "fun": minimize_constraint},
        )
        bounds = [(0, None) for _ in shares]
        solution = opt.minimize(
            optimize,
            shares,
            method="SLSQP",
            constraints=constraints,
            bounds=bounds,
        )

        potential_buy = np.floor(solution.x).astype(int)
        potential_cost = sum(potential_buy * prices)

        if potential_cost > 0 and funds - potential_cost > 0:
            return potential_buy

    return shares
