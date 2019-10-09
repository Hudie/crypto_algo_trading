from enum import Enum, unique

@unique
class Ecn(Enum):                               # ecn = electronic connection network, which includes exchange, otc, darkpool, etc
    binance           = 1
    deribit           = 2

@unique
class EcnStatus(Enum):                    
    init              = 1                      # service setup, yet trying to connect 
    connected         = 2            
    disconnected      = 3
    error             = 4                      # fail to connect or reconnect

@unique
class Broker(Enum):
    binance_dma       = 1                      # binance direct market access
    deribit_dma       = 2

@unique
class MarketDataApi(Enum):
    binance_md_restful   = 1
    binance_md_websocket = 2
    deribit_md_restful   = 3
    deribit_md_websocket = 4

@unique
class TradeDataApi(Enum):
    binance_td_restful   = 1
    binance_td_websocket = 2
    deribit_td_restful   = 3
    deribit_td_websocket = 4

@unique
class UserType(Enum):                          # typical rights include, code read, code write, money transfer, order read, order trade, admin, approval
    developer         = 1                      # code read, code write, order read
    trader            = 2                      # code read, order read, order trade
    treasury          = 3                      # order read, money transfer
    manager           = 4                      # admin, approval, but DEFINITELY no money transfer            

@unique
class ProductBaseType(Enum):                   # product vs instrument vs ticker, btc option is a product, btc-25-oct-10000-c is this particular option instrument's ticker
    stock             = 1
    fx                = 2
    bond              = 3
    commodity         = 4
    crypto            = 5

@unique
class ProductDerivativeType(Enum):  
    spot              = 1
    futures           = 2
    option            = 3
    perpetual         = 4

@unique
class Currency(Enum):                 
    btc               = 1
    eth               = 2
    usdt              = 3
    usdc              = 4
    usd               = 5
    cny               = 6
    jpy               = 7

@unique
class FeeType(Enum):                 
    percentage        = 1
    absolute          = 2
    deribit_option    = 3

class OptionType(Enum):
    call              = "C"
    put               = "P"  

@unique
class OrderDirection(Enum):
    buy               = 1
    sell              = -1 

@unique
class OrderType(Enum):
    limit             = "limit"
    market            = "market"

@unique
class OrderTimeInForce(Enum):
    gtc               = 1                      # good until cancel
    fok               = 2                      # fill or kill, no parital fills
    fak               = 3                      # fill and kill

@unique
class OrderStatus(Enum):
    pending           = 1
    acked             = 2
    reject            = 3
    cancel_pending    = 4
    canceled          = 5
    filled            = 6  
    partial_filled    = 7
 
def test():
    print (ProductBaseType.crypto)             # use object
    print (ProductBaseType.crypto.name)        # given object, get string name
    print (ProductBaseType.crypto.value)       # given object, get value
    print (ProductBaseType(5).name)            # given value, get string name
    print (ProductBaseType['crypto'].value)    # given name, get value

if __name__ == "__main__":
    test()
