#

import asyncio
from datetime import datetime
from typing import Iterable

import rich.console
import rich.layout
import rich.live
import rich.panel
import rich.progress
import rich.prompt
import rich.table
import rich.text
from stonkers.client import Client
from stonkers.convert import parse_ticker

from .account import AccountSummary
from .config import WheelConfig
from .formatting import boldize, colorize, number, titlize
from .positions import Positions
from .wheel import Wheel


def format_leg(leg: dict) -> str:
    """Formats a single leg of an order."""
    ticker_info = parse_ticker(leg["instrument"]["symbol"])
    if not ticker_info:
        return "Unrecognized ticker format"

    exp_date = datetime.strptime(ticker_info["expiration"], "%m%d%y")
    expiration = colorize(exp_date.strftime("%b %d, %Y"), "khaki3")

    contract_type = ticker_info["contract_type"].lower()
    contract_color = "orange3" if contract_type == "call" else "deep_pink3"
    contract = colorize(titlize(contract_type), contract_color)

    strike = f"{number(ticker_info['strike'], currency='$')}"
    instruction = colorize(titlize(leg["instruction"]), "bright_white")
    quantity = colorize(leg["quantity"], "chartreuse1")
    underlying = colorize(ticker_info["underlying"], "cyan1")

    return " ".join(
        (
            instruction,
            f"{quantity} x {underlying} {expiration} {strike} {contract}",
        )
    )


def format_order(order: dict, indent="") -> str:
    """Formats the order details including its legs."""
    order_type = titlize(order["orderType"])
    price = number(order["price"], currency="$")
    duration = titlize(order["duration"])
    leg_indent = indent * 2 if indent else "  "

    return "\n".join(
        (
            boldize(f"{indent}{order_type} {price} - {duration}"),
            "\n".join(
                (leg_indent + format_leg(leg))
                for leg in order["orderLegCollection"]
            ),
        ),
    )


def print_order(order: dict) -> str:
    """
    Prints the details of an order including its legs and child orders.
    """
    renderable = [format_order(order)]

    if "childOrderStrategies" in order:
        renderable.append(boldize(order["orderStrategyType"].capitalize()))
        renderable.extend(
            format_order(co, indent="  ")
            for co in order["childOrderStrategies"]
        )

    return "\n".join(renderable)


async def wheelie(  # pylint: disable=too-many-locals
    client: Client,
    account_id: str,
    tickers: Iterable[WheelConfig],
    auto_send_orders: bool = False,
):
    _tickers = dict((ticker.ticker, ticker) for ticker in set(tickers))

    console = rich.console.Console()

    with rich.live.Live(console=console, auto_refresh=False) as live:
        account = AccountSummary(
            account_id,
            client,
            update=lambda: live.update(account),
        )
        await account()

    with rich.live.Live(console=console) as live:
        positions = Positions(_tickers.keys(), lambda: live.update(positions))
        await positions(client, account_id)
        live.update(positions)

    with rich.progress.Progress(console=console) as progress:
        if _tickers:
            quote_task = progress.add_task(
                "Prefetching quotes...",
                total=1,
            )

            await client.quote(symbols=_tickers.keys())
            progress.advance(quote_task)

        wheel_task = progress.add_task(
            "Calculating target positions...",
            total=len(_tickers),
        )

        tasks = [
            Wheel(
                account_id,
                ticker,
                account_summary=account,
                client=client,
                positions=positions,
            )(progress, lambda: progress.advance(wheel_task))
            for ticker in _tickers.values()
        ]
        results = await asyncio.gather(*tasks)

    table = rich.table.Table()

    table.add_column("Ticker", justify="left", style="cyan")
    table.add_column("Status", justify="left", style="green")

    options = []
    for result in results:
        table.add_row(result.ticker, result)
        table.add_section()
        options.extend(result.options)

    console.print(table)

    orders = []
    if auto_send_orders:
        for option in options:
            orders.append(option.trigger_order().build())

    else:
        for option in options:
            order = option.trigger_order().build()

            console.print(print_order(order))

            if rich.prompt.Confirm.ask(
                "Send order?",
                default=False,
                console=console,
            ):
                orders.append(order)

    if not orders:
        return

    with rich.progress.Progress(console=console) as progress:
        task = progress.add_task(
            "Sending orders...",
            total=len(orders),
        )

        async def send_order(order):
            await client.place_order(account_id, order)
            progress.advance(task)

        await asyncio.gather(*[send_order(order) for order in orders])
