from cr_assis.account.accountBase import AccountBase
from cr_assis.connect.connectData import ConnectData
from pathlib import Path
import pandas as pd
import numpy as np
import ccxt

class AccountOkex(AccountBase):
    """Account Information only in Okex
    """
    
    def __init__(self, deploy_id: str) -> None:
        self.deploy_id = deploy_id
        self.exchange_position = "okexv5"
        self.empty_position:pd.DataFrame = pd.DataFrame(columns = ["usdt", "usdt-swap", "usdt-future", "usd-swap", "usd-future", "usdc-swap", "diff", "diff_U"])
        self.usd_position: pd.DataFrame = pd.DataFrame(columns = ["usd-swap"])
        self.script_path = str(Path( __file__ ).parent.parent.absolute())
        self.mongon_url = self.load_mongo_url()
        self.parameter_name = deploy_id.split("@")[0]
        self.client, self.username = self.parameter_name.split("_")
        self.database = ConnectData()
        self.markets = ccxt.okex().load_markets()
        self.contractsize_cswap : dict[str, float] = {"BTC": 100, "ETH": 10, "FIL": 10, "LTC": 10, "DOGE": 10, "ETC": 10}
        self.is_master = {"usd-future":0, "usd-swap":1, "usdc_swap":2, "usdt":3, "usdt-future":4, "usdt-swap":5}
    
    def get_contractsize_cswap(self, coin: str) ->float:
        coin = coin.upper()
        symbol = f"{coin}/USD:{coin}"
        contractsize = self.markets[symbol]["contractSize"] if symbol in self.markets.keys() else np.nan
        self.contractsize_cswap[coin] = contractsize
        return contractsize
    
    def get_influx_position(self, timestamp: str, the_time: str) -> pd.DataFrame:
        a = f"""
            select ex_field, long, long_open_price, settlement, short, short_open_price, pair from position 
            where client = '{self.client}' and username = '{self.username}' and 
            time > {the_time} - {timestamp} and time < {the_time} and (long >0 or short >0) 
            and exchange = '{self.exchange_position}' group by pair, ex_field, exchange ORDER BY time DESC LIMIT 1
            """
        data = self._send_complex_query(sql = a)
        data.dropna(subset = ["secret_id"], inplace= True) if "secret_id" in data.columns else None
        return data
    
    def gather_future_position(self, coin: str, raw_data: pd.DataFrame, col: str) -> float:
        """Gather different future contracts positions about this coin
        """
        data = raw_data[(raw_data["ex_field"] == "futures") & (raw_data["settlement"] == col.split("-")[0]) & (raw_data["coin"] == coin)].copy()
        data.drop_duplicates(subset = ["pair"], keep= "last", inplace= True)
        if col.split("-")[0] != "usd":
            amount = data["long"].sum() - data["short"].sum()
        else:
            contractsize = self.contractsize_cswap[coin] if coin in self.contractsize_cswap.keys() else self.get_contractsize_cswap(coin)
            amount = ((data["long"] - data["short"]) * contractsize / (data["long_open_price"] + data["short_open_price"])).sum()
        for coin in data.index:
            pair = data.loc[coin, "pair"].replace(coin.lower(), "")[1:]
            self.usd_position.loc[coin, pair] = data.loc[coin, "long"] - data.loc[coin, "short"]
        return amount
    
    def gather_coin_position(self, coin: str, all_data: pd.DataFrame) -> pd.DataFrame:
        """Gather positions of different contracts about this coin

        Args:
            coin (str): coin name, str.upper
            all_data (pd.DataFrame): origin position
        """
        result = self.empty_position.copy()
        coin = coin.upper()
        for col in result.columns:
            if "future" not in col and "diff" not in col:
                data = all_data[all_data["pair"] == f"{coin.lower()}-{col}"].copy()
                result.loc[coin, col] = (data["long"] - data["short"]).values[-1] if len(data) > 0 else 0
                if col.split("-")[0] == "usd" and result.loc[coin, col] != 0:
                    self.usd_position.loc[coin, col] = result.loc[coin, col]
                    contractsize = self.contractsize_cswap[coin] if coin in self.contractsize_cswap.keys() else self.get_contractsize_cswap(coin)
                    open_price = (data["long_open_price"] + data["short_open_price"]).values[-1] if len(data) > 0 else np.nan
                    result.loc[coin, col] = result.loc[coin, col] * contractsize / open_price if open_price != 0 else np.nan
            elif "future" == col.split("-")[-1]:
                result.loc[coin, col] = self.gather_future_position(coin = coin, raw_data = data, col = col)
            else:
                pass
        return result
        
    def gather_position(self) -> pd.DataFrame:
        """Gather the positions of different contracts of the same coin
        """
        data = self.origin_position if hasattr(self, "origin_position") and "pair" in self.origin_position.columns else pd.DataFrame(columns = ["pair"])
        data.sort_values(by = "time", inplace= True) if "time" in data.columns else None
        data["coin"] = data["pair"].apply(lambda x: x.split("-")[0].upper() if type(x) == str else "")
        coins = list(data["coin"].unique())
        result = self.empty_position.copy()
        for coin in coins:
            ret = self.gather_coin_position(coin = coin, all_data = data) if coin != "" else self.empty_position
            result = pd.concat([result, ret])
        return result
            
    def calculate_exposure(self):
        data = self.now_position.copy() if hasattr(self, "now_position") else self.empty_position.copy()
        cols = [x for x in self.empty_position.columns if "diff" not in x]
        data["diff"] = data[cols].sum(axis = 1)
        for coin in data.index:
            data.loc[coin, "diff_U"] = data.loc[coin, "diff"] * self.get_coin_price(coin)
        return data
    
    def get_coin_price(self, coin: str) -> float:
        ret = self.database.get_redis_data(key = f"{self.exchange_position}/{coin.lower()}-usdt")
        return float(ret[b'ask0_price']) if b'ask0_price' in ret.keys() else np.nan
    
    def get_now_position(self, timestamp="5m", the_time="now()") -> pd.DataFrame:
        the_time = f"'{the_time}'" if the_time != "now()" and "'" not in the_time else the_time
        self.origin_position = self.get_influx_position(timestamp = timestamp, the_time=the_time)
        self.now_position:pd.DataFrame = self.gather_position()
        self.now_position = self.calculate_exposure()
        return self.now_position.copy()
    
    def get_account_position(self):
        self.get_equity()
        data = self.get_now_position()
        