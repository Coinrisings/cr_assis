from cr_assis.account.accountBase import AccountBase
from cr_assis.connect.connectData import ConnectData
from pathlib import Path
import pandas as pd
import numpy as np
import ccxt, copy

class AccountOkex(AccountBase):
    """Account Information only in Okex
    """
    
    def __init__(self, deploy_id: str) -> None:
        self.deploy_id = deploy_id
        self.balance_id = self.deploy_id.replace("@", "-") + "@sum"
        self.exchange_position = "okexv5"
        self.exchange_combo = "okex"
        self.exchange_contract = "okex"
        self.folder = "pt"
        self.ccy = "BTC"
        self.principal_currency = "BTC"
        self.parameter: pd.DataFrame
        self.empty_position:pd.DataFrame = pd.DataFrame(columns = ["usdt", "usdt-swap", "usdt-future", "usd-swap", "usd-future", "usdc-swap", "diff", "diff_U"])
        self.open_price: pd.DataFrame = pd.DataFrame(columns = ["usdt", "usdt-swap", "usdt-future", "usd-swap", "usd-future", "usdc-swap"])
        self.now_price: pd.DataFrame = pd.DataFrame(columns = ["usdt", "usdt-swap", "usdt-future", "usd-swap", "usd-future", "usdc-swap"])
        self.usd_position: pd.DataFrame = pd.DataFrame(columns = ["usd-swap"])
        self.script_path = str(Path( __file__ ).parent.parent.absolute())
        self.mongon_url = self.load_mongo_url()
        self.parameter_name = deploy_id.split("@")[0]
        self.client, self.username = self.parameter_name.split("_")
        self.database = ConnectData()
        self.markets = ccxt.okex().load_markets()
        self.contractsize_uswap : dict[str, float] = {}
        self.cashBal : dict[str, float] = {}
        self.contractsize_cswap : dict[str, float] = {"BTC": 100, "ETH": 10, "FIL": 10, "LTC": 10, "DOGE": 10, "ETC": 10}
        self.exposure_number = 1
        self.is_master = {"usd-future":0, "usd-swap":1, "usdt":2, "usdc-swap":3, "usdt-future":4, "usdt-swap":5,  "": np.inf}
        self.secret_id = {"usd-future": "@okexv5:futures_usd", "usd-swap": "@okexv5:swap_usd", "usdc-swap": "@okexv5:swap_usdt",
                        "usdt": "@okexv5:spot", "usdt-future": "@okexv5:futures_usdt", "usdt-swap": "@okexv5:swap_usdt", "": ""}
        self.exchange_master, self.exchange_slave = "okex", "okex"
        self.path_orders = [f'{self.client}__{self.parameter_name}@okexv5_swap_usd', f'{self.client}__{self.parameter_name}@okexv5_swap_usdt']
        self.path_ledgers = [f'{self.client}__{self.parameter_name}@okexv5_swap_usd', f'{self.client}__{self.parameter_name}@okexv5_swap_usdt']
    
    def get_contractsize(self, symbol: str) -> float:
        return self.markets[symbol]["contractSize"] if symbol in self.markets.keys() else np.nan
    
    def get_pair_suffix_contract(self, contract: str) -> str:
        """get pair suffix from master or slave
        """
        contract = contract.replace("-", "_").replace("spot", "usdt")
        contract = contract.replace(contract.split("_")[0], "")
        return contract
    
    def get_pair_suffix(self, combo: str, future: str) -> tuple[str, str]:
        """get pair suffix from a combo
        """
        master, slave = combo.split("-")
        master, slave = self.get_pair_suffix_contract(master), self.get_pair_suffix_contract(slave)
        return master.replace("future", future), slave.replace("future", future)
    
    def get_pair_name(self, coin: str, combo: str) -> tuple[str, str]:
        master_suffix, slave_suffix = self.get_pair_suffix(combo, "future")
        return coin+master_suffix.replace("_", "-"), coin+slave_suffix.replace("_", "-")
    
    def get_secret_name(self, coin: str, combo: str) -> tuple[str, str]:
        master_suffix, slave_suffix = self.get_pair_suffix(combo, "future")
        return self.parameter_name+self.secret_id[master_suffix[1:].replace("_", "-")], self.parameter_name+self.secret_id[slave_suffix[1:].replace("_", "-")]
        
    def get_spreads(self, coin: str, combo: str, suffix="", start="now() - 24h", end = "now()") -> pd.DataFrame:
        """get spreads data
        Args:
            coin (str): coin name, str.lower
            combo (str): combo name, like "okx_usd_swap-okx_usdt_swap"
            suffix (str, optional): future delivery time. Defaults to "", which means this quarter
            start (str, optional): start time. Defaults to "now() - 24h".
            end (str, optional): end time. Defaults to "now()".

        Returns:
            pd.DataFrame: spreads data, columns = ["time", "ask0_spread", "bid0_spread", "dt"]
        """
        coin = coin.lower()
        master, slave = combo.split("-")
        start, end = self.get_influx_time_str(start), self.get_influx_time_str(end)
        suffix = self.get_quarter() if suffix == "" else suffix
        contract_master, contract_slave = self.get_pair_suffix(combo, future = suffix)
        kind_master = master.split("_")[-1].replace("future", "futures")
        kind_slave = slave.split("_")[-1].replace("future", "futures")
        database = self.get_all_database()
        dataname = f'''spread_orderbook_{self.exchange_contract}_{kind_master}_{coin}{contract_master}__orderbook_{self.exchange_contract}_{kind_slave}_{coin}{contract_slave}'''
        dataname_reverse = f'''spread_orderbook_{self.exchange_contract}_{kind_slave}_{coin}{contract_slave}__orderbook_{self.exchange_contract}_{kind_master}_{coin}{contract_master}'''
        is_exist = True
        if dataname in database:
            a = f"select time, ask0_spread, bid0_spread from {dataname} where time >= {start} and time <= {end}"
        elif dataname_reverse in database:
            a = f"select time, 1/ask0_spread as ask0_spread, 1/bid0_spread as bid0_spread from {dataname_reverse} where time >= {start} and time <= {end}"
        elif "future" in combo:
            if suffix in contract_master:
                dataname = f'''spread_orderbook_{self.exchange_contract}_{kind_master}_{coin}{contract_master}__orderbook_{self.exchange_contract}_spot_{coin}_usdt'''
                if dataname in database:
                    a = f"select time, ask0_spread, bid0_spread from {dataname} where time >= {start} and time <= {end}"
                else:
                    is_exist = False
            else:
                dataname_reverse = f'''spread_orderbook_{self.exchange_contract}_{kind_slave}_{coin}{contract_slave}__orderbook_{self.exchange_contract}_spot_{coin}_usdt'''
                if dataname_reverse in database:
                    a = f"select time, 1/ask0_spread as ask0_spread, 1/bid0_spread as bid0_spread from {dataname_reverse} where time >= {start} and time <= {end}"
                else:
                    is_exist = False
        else:
            is_exist = False
        spreads_data = pd.DataFrame(columns = ["time", "ask0_spread", "bid0_spread", "dt"])
        if not is_exist:
            print(f"{self.parameter_name} {combo} {coin} spreads database doesn't exist")
        else:
            return_data = self.database._send_influx_query(sql = a, database = "spreads", is_dataFrame= True)
            if len(return_data) > 0:
                spreads_data = return_data
        return spreads_data
        
    def get_contractsize_cswap(self, coin: str) ->float:
        coin = coin.upper()
        symbol = f"{coin}/USD:{coin}"
        contractsize = self.get_contractsize(symbol)
        self.contractsize_cswap[coin] = contractsize
        return contractsize
    
    def get_contractsize_uswap(self, coin: str) ->float:
        coin = coin.upper()
        symbol = f"{coin}/USDT:USDT"
        contractsize = self.get_contractsize(symbol)
        self.contractsize_uswap[coin] = contractsize
        return contractsize
    
    def get_influx_position(self, timestamp: str, the_time: str) -> pd.DataFrame:
        a = f"""
            select ex_field, secret_id,long, long_open_price, settlement, short, short_open_price, pair from position 
            where client = '{self.client}' and username = '{self.username}' and 
            time > {the_time} - {timestamp} and time < {the_time} and (long >0 or short >0) 
            and exchange = '{self.exchange_position}' group by pair, ex_field, exchange ORDER BY time DESC
            """
        data = self._send_complex_query(sql = a)
        data.dropna(subset = ["secret_id"], inplace= True) if "secret_id" in data.columns else None
        if len(data) == 0:
            a = f"""
            select ex_field, secret_id,long, long_open_price, settlement, short, short_open_price, pair from position 
            where client = '{self.client}' and username = '{self.username}' and 
            time > {the_time} - {timestamp} and time < {the_time}
            and exchange = '{self.exchange_position}' group by pair, ex_field, exchange ORDER BY time DESC
            """
            data = self._send_complex_query(sql = a)
            data.dropna(subset = ["secret_id"], inplace= True) if "secret_id" in data.columns else None
        data.drop_duplicates(subset= ["pair", "ex_field", "secret_id"], keep = "first", inplace = True)
        return data
    
    def find_future_position(self, coin: str, raw_data: pd.DataFrame, col: str) -> pd.DataFrame:
        data = raw_data[(raw_data["ex_field"] == "futures") & (raw_data["coin"] == coin)].copy()
        data["col"] = data["pair"].apply(lambda x: x.split("-")[1] if type(x) == str else None)
        data = data[data["col"] == col.split("-")[0]].copy()
        data["is_future"] = data["pair"].apply(lambda x: str.isnumeric(x.split("-")[-1]))
        data = data[data["is_future"] == True].copy()
        return data
    
    def gather_future_position(self, coin: str, raw_data: pd.DataFrame, col: str) -> float:
        """Gather different future contracts positions about this coin
        """
        data = self.find_future_position(coin, raw_data, col)
        data.drop_duplicates(subset = ["pair"], keep= "last", inplace= True)
        if col.split("-")[0] != "usd":
            amount = data["long"].sum() - data["short"].sum()
        else:
            contractsize = self.contractsize_cswap[coin] if coin in self.contractsize_cswap.keys() else self.get_contractsize_cswap(coin)
            amount = ((data["long"] - data["short"]) * contractsize / (data["long_open_price"] + data["short_open_price"])).sum()
        for i in data.index:
            coin = data.loc[i, "coin"]
            # pair = data.loc[i, "pair"].replace(coin.lower(), "")[1:]
            self.usd_position.loc[coin, col] = data.loc[i, "long"] - data.loc[i, "short"]
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
                result.loc[coin, col] = self.gather_future_position(coin = coin, raw_data = all_data, col = col)
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
    
    def is_ccy_exposure(self, array: pd.Series, contractsize: float) -> bool:
        other_array = array.drop("usdt").sort_values()
        not_spot = other_array.sum() > self.exposure_number * contractsize * 5 or other_array[0] + other_array[-1] > self.exposure_number * contractsize * 2
        is_spot = abs(other_array[0]) >= self.exposure_number * contractsize and abs(other_array[-1]) >= self.exposure_number * contractsize
        return not_spot and is_spot
    
    def tell_exposure(self) -> pd.DataFrame:
        data = self.now_position.copy() if hasattr(self, "now_position") else self.empty_position.copy()
        for coin in data.index:
            contractsize = self.contractsize_uswap[coin] if coin in self.contractsize_uswap.keys() else self.get_contractsize_uswap(coin)
            array = data.loc[coin].sort_values()
            array.drop(["diff", "diff_U"], inplace = True)
            array.drop(["is_exposure"], inplace = True) if "is_exposure" in array.index else None
            tell1 = np.isnan(data.loc[coin, "diff"])
            tell2 = abs(data.loc[coin, "diff"]) > self.exposure_number * contractsize * 6 if coin != self.ccy else self.is_ccy_exposure(array, contractsize)
            tell3 = abs(array[0] + array[-1]) > self.exposure_number * contractsize * 2 if coin != self.ccy else self.is_ccy_exposure(array, contractsize)
            data.loc[coin, "is_exposure"] = tell1 or tell2 or tell3
        data = pd.DataFrame(columns = list(self.empty_position.columns) + ["is_exposure"]) if len(data) == 0 else data
        return data
    
    def get_now_position(self, timestamp="10m", the_time="now()") -> pd.DataFrame:
        the_time = f"'{the_time}'" if "now()" not in the_time and "'" not in the_time else the_time
        self.origin_position = self.get_influx_position(timestamp = timestamp, the_time=the_time)
        self.now_position: pd.DataFrame = self.gather_position()
        self.now_position = self.calculate_exposure()
        self.now_position = self.tell_exposure()
        if self.deploy_id == "bg_001@dt_okex_cswap_okex_uswap_btc":
            self.now_position["is_exposure"] = False
            self.now_position["usdt"] = 0
        return self.now_position.copy()
    
    def get_open_price(self) -> pd.DataFrame:
        self.get_now_position() if not hasattr(self, "now_position") else None
        for coin in self.now_position.index:
            for contract in self.open_price.columns:
                if self.now_position.loc[coin, contract] != 0:
                    df = self.origin_position[self.origin_position["pair"] == f"{coin.lower()}-{contract}"].copy() if "future" not in contract else self.find_future_position(coin, self.origin_position, contract)
                    self.open_price.loc[coin, contract] = (df["long_open_price"] + df["short_open_price"]).values[-1] if len(df) > 0 else np.nan
                else:
                    self.open_price.loc[coin, contract] = 0
            if self.now_position.loc[coin, "usdt"] != 0:
                max_col = abs(self.now_position.loc[coin, self.open_price.columns.drop("usdt")]).sort_values().index[-1]
                self.open_price.loc[coin, "usdt"] = self.open_price.loc[coin, max_col]
        return self.open_price.copy()
    
    def get_cashBal(self, coin: str) -> float:
        a = f"""
        select last(origin) as origin FROM "equity_snapshot" WHERE time > now() - 10m and username = '{self.username}' and client = '{self.client}' and symbol = '{coin.lower()}'
        """
        ret = self.database._send_influx_query(a, database = "account_data", is_dataFrame= True)
        self.cashBal[coin.upper()] = float(eval(ret["origin"].values[-1])["cashBal"]) if len(ret) > 0 else np.nan
        return self.cashBal[coin.upper()]
    
    def get_now_price(self):
        self.get_now_position() if not hasattr(self, "now_position") else None
        for coin in set(self.now_position.index) | set([self.ccy]):
            self.now_price.loc[coin] = self.database.get_redis_okex_price(coin = coin.lower(), suffix="usdt")
    
    def tell_ccy_master(self, data: pd.Series, contractsize: float) -> dict[str, str]:
        other = data.drop("usdt").sort_values()
        ret = self.tell_master(other, contractsize)
        if "" in ret.values() and not self.now_position.loc[self.ccy, "is_exposure"]:
            ret = {"master":other[abs(other) == abs(other).max()].index[0], "slave": "usdt"} if abs(other).max() > self.exposure_number * contractsize else {"master": "", "slave": ""}
        return ret
    
    def tell_master(self, data: pd.Series, contractsize: float) -> dict[str, str]:
        data = data.sort_values()
        tell1 = abs(data[0] + data[-1]) <= contractsize * self.exposure_number and data[0] * data[-1] < 0
        tell2 = abs(data[1] + data[-1]) >= contractsize * self.exposure_number or data[1] * data[-1] > 0
        tell3 = abs(data[0] + data[-2]) >= contractsize * self.exposure_number or data[0] * data[-2] > 0
        result = [data.index[0], data.index[-1]] if tell1 and tell2 and tell3 else ["", ""]
        ret = {"master": result[0] if self.is_master[result[0]] < self.is_master[result[1]] else result[1],
                "slave": result[0] if self.is_master[result[0]] >= self.is_master[result[1]] else result[1]}
        return ret
    
    def transfer_pair(self, pair: str) -> str:
        ret = pair.replace("-", "_")
        ret = ret.replace("usdt", "spot") if ret.split("_")[-1] == "usdt" and "swap" not in ret else ret
        return ret
            
    def get_coin_combo(self, coin: str, master_pair: str, slave_pair: str) -> str:
        master = self.transfer_pair(master_pair.replace(coin.lower(), ""))
        slave = self.transfer_pair(slave_pair.replace(coin.lower(), ""))
        return f"{self.exchange_combo}{master}-{self.exchange_combo}{slave}"
    
    def get_account_position(self, timestamp = "10m", the_time = "now()") -> pd.DataFrame:
        self.get_equity()
        data = self.get_now_position(timestamp=timestamp, the_time=the_time)
        position = pd.DataFrame(columns = ["coin", "side", "position", "MV", "MV%", "master_pair", "slave_pair", "master_secret", "slave_secret", "combo"])
        data.drop(data[data["is_exposure"]].index, inplace= True)
        data.drop(["diff", "diff_U", "is_exposure"], axis=1, inplace=True)
        num = 0
        for coin in data.index:
            contractsize = self.contractsize_uswap[coin] if coin in self.contractsize_uswap.keys() else self.get_contractsize_uswap(coin)
            ret = self.tell_master(data = data.loc[coin], contractsize = contractsize) if coin != self.ccy else self.tell_ccy_master(data.loc[coin], contractsize)
            if ret["master"] == "" or ret["slave"] == "":
                continue
            else:
                position.loc[num, "coin"] = coin.lower()
                position.loc[num, "side"] = "long" if data.loc[coin, ret["master"]] > 0 else "short"
                position.loc[num, "position"] = abs(data.loc[coin, ret["master"]]) if ret["master"].split("-")[0] != "usd" else abs(self.usd_position.loc[coin, ret["master"]])
                position.loc[num, "MV"] = position.loc[num, "position"] * self.get_coin_price(coin = coin.lower()) if ret["master"].split("-")[0] != "usd" else position.loc[num, "position"] * self.contractsize_cswap[coin]
                position.loc[num, "MV%"] = round(position.loc[num, "MV"] / self.adjEq * 100, 4)
                position.loc[num, ["master_pair", "slave_pair"]] = [f'{position.loc[num, "coin"]}-{ret["master"]}', f'{position.loc[num, "coin"]}-{ret["slave"]}']
                position.loc[num, ["master_secret", "slave_secret"]] = [f'{self.parameter_name}{self.secret_id[ret["master"]]}', f'{self.parameter_name}{self.secret_id[ret["slave"]]}']
                position.loc[num, "combo"] = self.get_coin_combo(coin, position.loc[num, "master_pair"], position.loc[num, "slave_pair"])
                num += 1
        position.drop(position[(position["master_secret"] == self.parameter_name) | (position["master_secret"] == self.parameter_name)].index, inplace= True)
        position.sort_values(by = "MV%", ascending= False, inplace= True)
        position.index = range(len(position))
        self.position = position.copy()
        return position