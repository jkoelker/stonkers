#

from tda import orders  # type: ignore


def truncate_float(value, precision=2) -> str:
    return f"{value:.{precision}f}"


class OrderBuilder(orders.generic.OrderBuilder):
    # pylint: disable=too-many-public-methods

    OrderType = orders.common.OrderType
    Duration = orders.common.Duration
    Session = orders.common.Session
    ComplexOrderStrategyType = orders.common.ComplexOrderStrategyType
    StopPriceLinkBasis = orders.common.StopPriceLinkBasis
    StopPriceLinkType = orders.common.StopPriceLinkType
    StopType = orders.common.StopType
    PriceLinkBasis = orders.common.PriceLinkBasis
    PriceLinkType = orders.common.PriceLinkType
    OrderStrategyType = orders.common.OrderStrategyType

    EquityInstrument = orders.common.EquityInstrument
    OptionInstrument = orders.common.OptionInstrument

    EquityInstruction = orders.common.EquityInstruction
    OptionInstruction = orders.common.OptionInstruction
    SpecialInstruction = orders.common.SpecialInstruction

    OptionSymbol = orders.options.OptionSymbol

    def __repr__(self):
        return f"OrderBuilder({self.build()})"

    def set_stop_price(self, stop_price):
        if isinstance(stop_price, str):
            self._stopPrice = stop_price
        else:
            self._stopPrice = truncate_float(stop_price)

        return self

    def set_price(self, price):
        if isinstance(price, str):
            self._price = price
        else:
            self._price = truncate_float(price)

        return self

    @property
    def session(self):
        return self._session

    @session.setter
    def session(self, value):
        self.set_session(value)

    @session.deleter
    def session(self):
        self.clear_session()

    @property
    def duration(self):
        return self._duration

    @duration.setter
    def duration(self, value):
        self.set_duration(value)

    @duration.deleter
    def duration(self):
        self.clear_duration()

    @property
    def order_type(self):
        return self._orderType

    @order_type.setter
    def order_type(self, value):
        self.set_order_type(value)

    @order_type.deleter
    def order_type(self):
        self.clear_order_type()

    @property
    def complex_order_strategy_type(self):
        return self._complexOrderStrategyType

    @complex_order_strategy_type.setter
    def complex_order_strategy_type(self, value):
        self.set_complex_order_strategy_type(value)

    @complex_order_strategy_type.deleter
    def complex_order_strategy_type(self):
        self.clear_complex_order_strategy_type()

    @property
    def quantity(self):
        return self._quantity

    @quantity.setter
    def quantity(self, value):
        self.set_quantity(value)

    @quantity.deleter
    def quantity(self):
        self.clear_quantity()

    @property
    def requested_destination(self):
        return self._requestedDestination

    @requested_destination.setter
    def requested_destination(self, value):
        self.set_requested_destination(value)

    @requested_destination.deleter
    def requested_destination(self):
        self.clear_requested_destination()

    @property
    def stop_price(self):
        return self._stopPrice

    @stop_price.setter
    def stop_price(self, value):
        self.set_stop_price(value)

    @stop_price.deleter
    def stop_price(self):
        self.clear_stop_price()

    @property
    def stop_price_link_basis(self):
        return self._stopPriceLinkBasis

    @stop_price_link_basis.setter
    def stop_price_link_basis(self, value):
        self.set_stop_price_link_basis(value)

    @stop_price_link_basis.deleter
    def stop_price_link_basis(self):
        self.clear_stop_price_link_basis()

    @property
    def stop_price_link_type(self):
        return self._stopPriceLinkType

    @stop_price_link_type.setter
    def stop_price_link_type(self, value):
        self.set_stop_price_link_type(value)

    @stop_price_link_type.deleter
    def stop_price_link_type(self):
        self.clear_stop_price_link_type()

    @property
    def stop_price_offset(self):
        return self._stopPriceOffset

    @stop_price_offset.setter
    def stop_price_offset(self, value):
        self.set_stop_price_offset(value)

    @stop_price_offset.deleter
    def stop_price_offset(self):
        self.clear_stop_price_offset()

    @property
    def stop_type(self):
        return self._stopType

    @stop_type.setter
    def stop_type(self, value):
        self.set_stop_type(value)

    @stop_type.deleter
    def stop_type(self):
        self.clear_stop_type()

    @property
    def price_link_basis(self):
        return self._priceLinkBasis

    @price_link_basis.setter
    def price_link_basis(self, value):
        self.set_price_link_basis(value)

    @price_link_basis.deleter
    def price_link_basis(self):
        self.clear_price_link_basis()

    @property
    def price_link_type(self):
        return self._priceLinkType

    @price_link_type.setter
    def price_link_type(self, value):
        self.set_price_link_type(value)

    @price_link_type.deleter
    def price_link_type(self):
        self.clear_price_link_type()

    @property
    def price(self):
        return self._price

    @price.setter
    def price(self, value):
        self.set_price(value)

    @price.deleter
    def price(self):
        self.clear_price()

    @property
    def activation_price(self):
        return self._activationPrice

    @activation_price.setter
    def activation_price(self, value):
        self.set_activation_price(value)

    @activation_price.deleter
    def activation_price(self):
        self.clear_activation_price()

    @property
    def special_instruction(self):
        return self._specialInstruction

    @special_instruction.setter
    def special_instruction(self, value):
        self.set_special_instruction(value)

    @special_instruction.deleter
    def special_instruction(self):
        self.clear_special_instruction()

    @property
    def order_strategy_type(self):
        return self._orderStrategyType

    @order_strategy_type.setter
    def order_strategy_type(self, value):
        self.set_order_strategy_type(value)

    @order_strategy_type.deleter
    def order_strategy_type(self):
        self.clear_order_strategy_type()

    @classmethod
    def base_order(cls):
        return (
            cls()
            .set_session(cls.Session.NORMAL)
            .set_duration(cls.Duration.DAY)
            .set_order_strategy_type(cls.OrderStrategyType.SINGLE)
        )

    @classmethod
    def market_order(cls):
        return cls().base_order().set_order_type(cls.OrderType.MARKET)

    @classmethod
    def limit_order(cls, price):
        return (
            cls()
            .base_order()
            .set_order_type(cls.OrderType.LIMIT)
            .set_price(price)
        )

    @classmethod
    def option_buy_to_open_market(cls, symbol, quantity):
        return (
            cls()
            .market_order()
            .add_option_leg(
                cls.OptionInstruction.BUY_TO_OPEN, symbol, quantity
            )
        )

    @classmethod
    def option_buy_to_open_limit(cls, symbol, quantity, price):
        return (
            cls()
            .limit_order(price)
            .add_option_leg(
                cls.OptionInstruction.BUY_TO_OPEN, symbol, quantity
            )
        )

    @classmethod
    def option_sell_to_open_market(cls, symbol, quantity):
        return (
            cls()
            .market_order()
            .add_option_leg(
                cls.OptionInstruction.SELL_TO_OPEN, symbol, quantity
            )
        )

    @classmethod
    def option_sell_to_open_limit(cls, symbol, quantity, price):
        return (
            cls()
            .limit_order(price)
            .add_option_leg(
                cls.OptionInstruction.SELL_TO_OPEN, symbol, quantity
            )
        )

    @classmethod
    def option_buy_to_close_market(cls, symbol, quantity):
        return (
            cls()
            .market_order()
            .add_option_leg(
                cls.OptionInstruction.BUY_TO_CLOSE, symbol, quantity
            )
        )

    @classmethod
    def option_buy_to_close_limit(cls, symbol, quantity, price):
        return (
            cls()
            .limit_order(price)
            .add_option_leg(
                cls.OptionInstruction.BUY_TO_CLOSE, symbol, quantity
            )
        )

    @classmethod
    def option_sell_to_close_market(cls, symbol, quantity):
        return (
            cls()
            .market_order()
            .add_option_leg(
                cls.OptionInstruction.SELL_TO_CLOSE, symbol, quantity
            )
        )

    @classmethod
    def option_sell_to_close_limit(cls, symbol, quantity, price):
        return (
            cls()
            .limit_order(price)
            .add_option_leg(
                cls.OptionInstruction.SELL_TO_CLOSE, symbol, quantity
            )
        )

    @classmethod
    def equity_buy_market(cls, symbol, quantity):
        return (
            cls()
            .market_order()
            .add_equity_leg(cls.EquityInstruction.BUY, symbol, quantity)
        )
