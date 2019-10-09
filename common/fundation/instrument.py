from constant import Currency, ProductBaseType, ProductDerivativeType, FeeType, OptionType

class CryptoOption(object):

    def __init__(self, ticker, option_type, strike, expriation_timestamp, creation_timestamp):
        self.ticker                        = ticker
        self.product_base_type             = ProductBaseType.crypto
        self.product_derivative_type       = ProductDerivativeType.option
        self.base_currency                 = Currency.btc
        self.quote_currency                = Currency.usd
        self.option_type                   = option_type
        self.strike                        = strike
        self.expriation_timestamp          = expriation_timestamp
        self.creation_timestamp            = creation_timestamp
        self.tick_size                     = 0.0005
        self.min_trade_size                = 0.1
        self.taker_fee                     = 0.0
        self.maker_fee                     = 0.0
        self.fee_type                      = FeeType.deribit_option
        self.is_active                     = False

    def __str__(self):
        return 'crypto_option : %s' % (self.__dict__)

class CryptoFutures(object):
    
    def __init__(self, ticker, expriation_timestamp, creation_timestamp):
        self.ticker                        = ticker
        self.product_base_type             = ProductBaseType.crypto
        self.product_derivative_type       = ProductDerivativeType.futures
        self.base_currency                 = Currency.btc
        self.quote_currency                = Currency.usd
        self.expriation_timestamp          = expriation_timestamp
        self.creation_timestamp            = creation_timestamp
        self.tick_size                     = 0.5
        self.min_trade_size                = 10.0
        self.taker_fee                     = 0.0005
        self.maker_fee                     = -0.0002
        self.fee_type                      = FeeType.percentage
        self.is_active                     = False

    def __str__(self):
        return 'crypto_futures : %s' % (self.__dict__)

class CryptoPerpetual(object):
    
    def __init__(self, ticker, expriation_timestamp, creation_timestamp):
        self.ticker                        = ticker
        self.product_base_type             = ProductBaseType.crypto
        self.product_derivative_type       = ProductDerivativeType.perpetual
        self.base_currency                 = Currency.btc
        self.quote_currency                = Currency.usd
        self.expriation_timestamp          = expriation_timestamp
        self.creation_timestamp            = creation_timestamp
        self.tick_size                     = 0.5
        self.min_trade_size                = 10.0
        self.taker_fee                     = 0.00075
        self.maker_fee                     = -0.00025
        self.fee_type                      = FeeType.percentage
        self.is_active                     = False

    def __str__(self):
        return 'crypto_perpetual : %s' % (self.__dict__)

def test():
    option = CryptoOption("BTC-25OCT-10000-C", OptionType.call, 10000, 0.0, 0.0)
    print (option)
    
if __name__ == "__main__":
    test()
