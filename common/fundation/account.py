from constant import Broker, MarketDataApi, TradeDataApi

class CryptoTradingAccount(object):

    def __init__(self,
                 label,              # internal label
                 broker,             # the broker or exchange dma of the account
                 id,                 # account id
                 pwd,                # account pwd
                 md_api,             
                 td_api,
                 api_public_key,
                 api_private_key  
                 ):

        self.label           = label
        self.broker          = broker
        self.id              = id
        self.pwd             = pwd
        self.md_api          = md_api
        self.td_api          = td_api
        self.api_public_key  = api_public_key
        self.api_private_key = api_private_key
    
    def __str__(self):
        return 'account : %s' % (self.__dict__)

def test():
    account = CryptoTradingAccount("deribit_fpt_000", Broker.deribit_dma, "fpt_000", "pass", MarketDataApi.deribit_md_restful, TradeDataApi.deribit_td_restful, None, None)
    print (account)
    
if __name__ == "__main__":
    test()
