#

from typing import Optional


def titlize(text: str) -> str:
    return text.replace("_", " ").title()


def boldize(text: str) -> str:
    return f"[bold]{text}[/bold]"


def colorize(text: str, color: str = "green", bold: bool = False) -> str:
    if bold:
        text = boldize(text)

    return f"[{color}]{text}[/{color}]"


def number(
    value: float | str,
    precision: int = 2,
    percent: bool = False,
    currency: str = "",
    bold: bool = True,
    color: Optional[str] = None,
) -> str:
    if isinstance(value, str):
        value = float(value)

    _color = color if color else "green" if value >= 0 else "red"

    def fmt(value: float, pre: str = "", post: str = "") -> str:
        value_str = f"{value:,.{precision}f}"

        if precision > 2:
            # NOTE(jkoelker) Remove all trailing zeros.
            value_str = value_str.rstrip("0")

            # NOTE(jkoelker) If the last character is a decimal point,
            # pad up to 2 decimal places with zeros.
            while len(value_str.split(".")[-1]) < 2:
                value_str += "0"

        return colorize(f"{pre}{value_str}{post}", color=_color, bold=bold)

    if percent:
        value *= 100

    return fmt(
        value, pre=currency if currency else "", post="%" if percent else ""
    )


def join(*args: str, sep: str = " ") -> str:
    return sep.join(args)
