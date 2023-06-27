
from cr_assis.pnl.okexPnl import OkexPnl
from binance.um_futures import UMFutures
from cr_assis.load import *
from cr_assis.connect.connectData import ConnectData
from binance.cm_futures import CMFutures

class BinancePnl(OkexPnl):
    
    def __init__(self):
        self.database = ConnectData()
        self.ccy = "USDT"
        self.interval = "8H"
        self.exchange = "binance"
        self.slip_unit = 10000
        self.real_uswap = 0.0004
        self.real_cswap = 0.0001
        self.fake_uswap = 0.00013
        self.fake_cswap = -0.00009
        self.api_info = {}
        self.get_accounts()
    
    def get_accounts(self) -> None:
        with open(f"{os.environ['HOME']}/.cr_assis/account_binance_api.yml", "rb") as f:
            data: list[dict] = yaml.load(f, Loader= yaml.SafeLoader)
        self.accounts = [i['name'] for i in data if "hf" == i['name'].split("_")[0]]
        for i in data:
            self.api_info[i["name"]] = {"api_key": i["api_key"], "secret_key": i["secret_key"]}
    
    def load_api_uswap(self, name: str) -> None:
        self.api_uswap = UMFutures(self.api_info[name]["api_key"], self.api_info[name]["secret_key"])
    
    def load_api_cswap(self, name: str) -> None:
        self.api_cswap = CMFutures(self.api_info[name]["api_key"], self.api_info[name]["secret_key"], base_url="https://dapi.binance.com")
    
    def get_long_bills(self, name: str, start: datetime.datetime, end: datetime.datetime) -> pd.DataFrame:
        start = self.dt_to_ts(start)
        ts = self.dt_to_ts(end)
        self.load_api_uswap(name)
        data = []
        while ts >= start:
            print(ts)
            response = self.api_uswap.get_account_trades(symbol = "BTCUSDT", recvWindow=6000, endTime = ts)
            data += response
            if len(response) > 0:
                ts = response[0]["time"]
            else:
                break
        ts = self.dt_to_ts(end)
        self.load_api_cswap(name)
        while ts >= start:
            response = self.api_cswap.get_account_trades(pair = "BTCUSD", recvWindow=6000, endTime = ts)
            data += response
            if len(response) > 0:
                ts = response[0]["time"]
            else:
                break
        df = pd.DataFrame(data).drop_duplicates().sort_values(by = "time")
        df["dt"] = df["time"].apply(lambda x: datetime.datetime.fromtimestamp(x/1000))
        cols = ["price", "qty", "realizedPnl", "quoteQty", "commission", "baseQty"]
        df[cols] = df[cols].astype(float)
        return df