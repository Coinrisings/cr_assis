import pandas as pd
import numpy as np
import datetime, time, os, yaml, json
from pymongo import MongoClient
from connectData import ConnectData
from research.utils.ObjectDataType import AccountData
from pathlib import Path

class AccountBase(object):
    def __init__(self,  deploy_id: str, strategy = "funding") -> None:
        # 要求同一个账户里单一币种的master和slave是唯一的
        self.deploy_id = deploy_id
        self.strategy = strategy
        self.script_path = str(Path( __file__ ).parent.absolute())
        self.mongon_url = self.load_mongo_url()
        self.init_account(self.deploy_id)
    
    
    def load_mongo_url(self):
        with open(f"{os.environ['HOME']}/.cryptobridge/private_key.yml") as f:
            data = yaml.load(f, Loader= yaml.SafeLoader)
        for info in data:
            if "mongo" in info:
                mongo_uri = info["mongo"]
                return mongo_uri
    
    def get_strategy_info(self, strategy: str):
        """解析deployd_id的信息"""
        words = strategy.split("_")
        master = (words[1] + "_" + words[2]).replace("okexv5", "okx").replace("okex", "okx")
        master = master.replace("uswap", "usdt_swap")
        master = master.replace("cswap", "usd_swap")
        master = master.replace("ufuture", "usdt_future")
        master = master.replace("cfuture", "usd_future")
        slave = (words[3] + "_" + words[4]).replace("okexv5", "okx").replace("okex", "okx")
        slave = slave.replace("uswap", "usdt_swap")
        slave = slave.replace("cswap", "usd_swap")
        slave = slave.replace("ufuture", "usdt_future")
        slave = slave.replace("cfuture", "usd_future")
        ccy = words[-1].upper()
        if ccy == "U":
            ccy = "USDT"
        elif ccy == "C":
            ccy = "BTC"
        else:
            pass
        return master, slave, ccy

    def get_bbu_info(self, strategy: str):
        """解析bbu线的deploy_id信息"""
        words = strategy.split("_")
        exchange = words[1].replace("okex", "okx")
        if exchange == "binance":
            master = "binance_busd_swap"
        else:
            master = f"{exchange}_usdc_swap"
        slave = f"{exchange}_usdt_swap"
        ccy = strategy.split("_")[-1].upper()
        if ccy in ["U", "BUSD"]:
            ccy = "USDT"
        else:
            pass
        return master, slave, ccy
    
    def init_account(self, deploy_id: str) -> None:
        #initilize account's info by deploy_id
        self.parameter_name, strategy = deploy_id.split("@")
        self.client, self.username = self.parameter_name.split("_")
        words = strategy.split("_")
        if words[1] != words[3] or words[2] != words[4]:
            self.master, self.slave, self.ccy = self.get_strategy_info(strategy)
        elif words[2] == "uswap" and words[0] == "h3f":
            self.master, self.slave, self.ccy = self.get_bbu_info(strategy)
        else:
            print(f"This deploy id cannot be analyzed: {deploy_id}")
            return 
        self.initilize()
    
    def initilize(self):
        self.database = ConnectData()
        self.principal_currency = self.ccy
        self.combo = self.master + "-" + self.slave
        self.exchange_master = self.master.split("_")[0]
        self.exchange_slave = self.slave.split("_")[0]
        self.contract_master = self.master.replace(self.exchange_master, "")
        self.contract_slave = self.slave.replace(self.exchange_slave, "")
        self.exchange_master = self.unified_exchange_name(self.exchange_master)
        self.exchange_slave = self.unified_exchange_name(self.exchange_slave)
        self.contract_master = self.unified_suffix(self.contract_master)
        self.contract_slave = self.unified_suffix(self.contract_slave)
        self.kind_master = self.exchange_master + self.contract_master
        self.kind_slave = self.exchange_slave + self.contract_slave
        self.get_folder()
        self.contractsize = pd.read_csv(f"{os.environ['HOME']}/parameters/config_buffet/dt/contractsize.csv", index_col = 0)
        self.balance_id = self.deploy_id.replace("@", "-") + "@sum"
        data = self.get_now_parameter()
        secret_slave = data.loc[0, "secret_slave"]
        slave = secret_slave.split("@")[0].split("/")[-1]
        self.slave_client, self.slave_username = slave.split("_")
        path1 = data.loc[0, "secret_master"].replace("/", "__")
        path1 = path1.replace(":", "_")
        path2 = data.loc[0, "secret_slave"].replace("/", "__")
        path2 = path2.replace(":", "_")
        paths = [path1, path2]
        self.path_orders = paths
        self.path_ledgers = paths
    
    def get_folder(self):
        # parameter folder in GitHub
        self.folder = self.deploy_id.split("@")[-1].split("_")[0]
        
    def get_quarter(self):
        today = datetime.date.today()
        quarter = str(today.year)[-2:]
        last_day = ""
        month = today.month
        if month <= 3:
            last_day = "0331"
        elif month <= 6:
            last_day = "0630"
        elif month <= 9:
            last_day = "0930"
        else:
            last_day = "1231"
        quarter = quarter + last_day
        return quarter
    
    def get_all_database(self, db = "spreads") -> set:
        """get list all measurements in INFLUXDB

        Args:
            db (str, optional): database name. Defaults to "spreads".

        Returns:
            set: list of name, {'spread_orderbook_binance_spot_audio_usdt__orderbook_binance_swap_audio_usdt_swap__reverse',
            'spread_orderbook_hbg_futures_bch_usd_220819__orderbook_hbg_spot_bch_usdt'}
        """
        self.database.load_influxdb(database = db)
        influx_client = self.database.influx_clt
        measurements = influx_client.get_list_measurements()
        database = set()
        for info in measurements:
            name = info["name"]
            database.add(name)
        return database
    
    def get_spreads(self, coin: str, hours = 24, suffix = "", time_unit = "h") -> pd.DataFrame: 
        """get spreads data in last serveral hours

        Args:
            coin (str): the coin name
            hours (int, optional): spreads time. Defaults to 24.
            suffix (str, optional): delivery data, only for future. Defaults to "".
            time_unit (str, optional): spreads time unit, Defaults to h.

        Returns:
            pd.DataFrame: spreads data, columns = ["time", "ask0_spread", "bid0_spread", "dt"]
        """
        coin = coin.lower()
        if suffix == "":
            suffix = self.get_quarter()
        exchange_master = self.exchange_master
        exchange_slave = self.exchange_slave
        contract_master = self.contract_master.replace("-", "_").replace("future", suffix)
        contract_slave = self.contract_slave.replace("-", "_").replace("future", suffix)
        kind_master = self.master.split("_")[-1].replace("future", "futures")
        kind_slave = self.slave.split("_")[-1].replace("future", "futures")
        if exchange_master in ["okx", "okexv5"]:
            exchange_master = "okex"
        if exchange_slave in ["okx", "okexv5"]:
            exchange_slave = "okex"
        database = self.get_all_database()
        dataname = f'''spread_orderbook_{exchange_master}_{kind_master}_{coin}{contract_master}__orderbook_{exchange_slave}_{kind_slave}_{coin}{contract_slave}'''
        dataname_reverse = f'''spread_orderbook_{exchange_slave}_{kind_slave}_{coin}{contract_slave}__orderbook_{exchange_master}_{kind_master}_{coin}{contract_master}'''
        is_exist = True
        if dataname in database:
            a = f"select time, ask0_spread, bid0_spread from {dataname} where time >= now() - {hours}{time_unit}"
        elif dataname_reverse in database:
            a = f"select time, 1/ask0_spread as ask0_spread, 1/bid0_spread as bid0_spread from {dataname_reverse} where time >= now() - {hours}{time_unit}"
        elif "future" in self.combo:
            if suffix in contract_master:
                dataname = f'''spread_orderbook_{exchange_master}_{kind_master}_{coin}{contract_master}__orderbook_{exchange_slave}_spot_{coin}_usdt'''
                if dataname in database:
                    a = f"select time, ask0_spread, bid0_spread from {dataname} where time >= now() - {hours}{time_unit}"
                else:
                    is_exist = False
            else:
                dataname_reverse = f'''spread_orderbook_{exchange_slave}_{kind_slave}_{coin}{contract_slave}__orderbook_{exchange_master}_spot_{coin}_usdt'''
                if dataname_reverse in database:
                    a = f"select time, 1/ask0_spread as ask0_spread, 1/bid0_spread as bid0_spread from {dataname_reverse} where time >= now() - {hours}{time_unit}"
                else:
                    is_exist = False
        else:
            is_exist = False
        spreads_data = pd.DataFrame(columns = ["time", "ask0_spread", "bid0_spread", "dt"])
        if not is_exist:
            print(f"{self.parameter_name} spreads database doesn't exist")
        else:
            return_data = self.database._send_influx_query(sql = a, database = "spreads", is_dataFrame= True)
            if len(return_data) > 0:
                spreads_data = return_data
        return spreads_data
    
    def get_account_position(self):
        self.get_equity()
        self.get_now_position()
        now_position = self.now_position.copy()
        now_position = now_position.dropna()
        if len(now_position) == 0:
            data = self.get_now_position(timestamp = "12h")
            
            if len(data) > 0:
                num = 0
                print(f"Failed to get {self.parameter_name} position at {datetime.datetime.now()}")
                return 
        data = pd.DataFrame(columns =["coin", "side", "position", "MV", "MV%"], index = range(len(now_position)))
        for i in data.index:
            coin = now_position.index.values[i]
            data.loc[i, "coin"] = coin
            data.loc[i, "side"] = now_position.loc[coin, "side"]
            data.loc[i, "position"] = now_position.loc[coin, "master_number"]
            data.loc[i, "MV"] = round(now_position.loc[coin, "master_MV"], 3)
            data.loc[i, "MV%"] = round(data.loc[i, "MV"] / self.adjEq * 100, 3)
        if "MV" in data.columns:
            data = data.sort_values(by = "MV", ascending = False) 
        data = data.dropna(axis = 0, how = "all")
        data.index = range(len(data))
        self.position = data.copy()
        
    def get_coin_price(self, coin, kind = "") -> float:
        """kind: exchange + contract type, for example "okex_spot", "okex_usdt_swap" """
        
        if kind == "":
            kind = self.exchange_master + "_spot"
        kind = kind.replace("-", "_")
        exchange = kind.split("_")[0]
        x = kind.split("_")[-1]
        if x == "swap":
            suffix = "-" + kind.split("_")[-2] + "-" + kind.split("_")[-1]
        elif x in ["spot", "usdt"]:
            suffix = "-usdt"
        else:
            print("get_coin_price: coin price kind error")
            print(f"kind: {kind}")
            print(f"x: {x}")
            return np.nan
        
        if exchange in ["okx", "okex", "okex5", "ok", "o", "okexv5"]:
            exchange = "okexv5"
        elif exchange in ["binance", "b"]:
            exchange = "binance"
        elif exchange in ["gateio", "g", "gate"]:
            exchange = "gate"
        elif exchange in ["hbg", "h"]:
            exchange = "hbg"
        else:
            print("get_coin_price: coin price exchange error")
            return np.nan
        key = f"{exchange}/{coin}{suffix}"
        self.database.load_redis()
        data = self.database.get_redis_data(key)
        self.database.redis_clt.close()
        if b'bid0_price' in data.keys():
            price = float(data[b'bid0_price'])
        else:
            price = np.nan
        return price
    
    def get_equity(self):
        data = pd.DataFrame()
        num = 0
        while len(data) == 0:
            dataname = "balance_v2"
            a = f'''
            select usdt,balance_id,time from {dataname} where time > now() - 10m and balance_id = '{self.balance_id}'
            order by time desc
            '''
            self.database.load_influxdb(database = "account_data")
            ret = self.database.influx_clt.query(a)
            data = pd.DataFrame(ret.get_points())
            self.database.influx_clt.close()
            if "balance_id" in data.columns:
                data["true"] = data["balance_id"].apply(lambda x: True if type(x) == type("a") else False)
                data = data[data["true"]]
            else:
                data = pd.DataFrame()
                self.adjEq = np.nan
            if len(data) == 0 and num < 10:
                num += 1
                print(f"{self.parameter_name} failed to get equity at {datetime.datetime.now()}: {num}")
                time.sleep(1)
            elif num >= 10:
                print(f"give up getting {self.parameter_name} equity at {datetime.datetime.now()}")
                return 
        self.adjEq = data.usdt.values[0]
        
    def get_capital(self, time = "None"):
        if self.exchange_master in ["okex", "okx", "okex5", "okexv5"] and self.exchange_slave in ["okex", "okx", "okex5", "okexv5"]:
            dataname = "equity_snapshot"
            if time == "None":
                a = f"""
                select origin, time from {dataname} where username = '{self.username}' and client = '{self.client}' and symbol = '{self.principal_currency.lower()}' order by time desc LIMIT 1
                """
            else:
                a = f"""
                select origin, time from {dataname} where username = '{self.username}' and client = '{self.client}' and symbol = '{self.principal_currency.lower()}' and time <= '{time}' order by time desc LIMIT 1
                """
            self.database.load_influxdb(database = "account_data")
            ret = self.database.influx_clt.query(a)
            self.database.influx_clt.close()
            data = pd.DataFrame(ret.get_points())
            origin_data = eval(data.origin.values[-1])
            capital = eval(origin_data["eq"])
            self.capital = capital
            capital_price = eval(origin_data["eqUsd"]) / capital
            self.capital_price = capital_price
        else:
            dataname = "equity"
            if time == "None":
                a = f"""
                select {self.principal_currency.lower()}, time from {dataname} where username = '{self.username}' and client = '{self.client}'  order by time desc LIMIT 1
                """
            else:
                a = f"""
                select {self.principal_currency.lower()}, time from {dataname} where username = '{self.username}' and client = '{self.client}'  and time <= '{time}' order by time desc LIMIT 1
                """
            self.database.load_influxdb(database = "account_data")
            ret = self.database.influx_clt.query(a)
            self.database.influx_clt.close()
            data = pd.DataFrame(ret.get_points())
            capital = data.loc[0, self.principal_currency.lower()]
            if self.principal_currency.upper() not in ["USDT", "USD", "BUSD"]:
                capital_price = self.get_coin_price(coin = self.principal_currency.lower())
            else:
                capital_price = 1
            self.capital = capital
            self.capital_price = capital_price
        return capital, capital_price
    
    def get_upnl(self, time = "None"):
        if self.exchange_master in ["okex", "okx", "okex5", "okexv5"] and self.exchange_slave in ["okex", "okx", "okex5", "okexv5"]:
            dataname = "equity_snapshot"
            if time == "None":
                a = f"""
                select origin, symbol, time from {dataname} where username = '{self.username}' and client = '{self.client}' and symbol = '{self.principal_currency.lower()}' order by time desc LIMIT 1
                """
            else:
                a = f"""
                select origin, symbol, time from {dataname} where username = '{self.username}' and client = '{self.client}' and symbol = '{self.principal_currency.lower()}' and time <= '{time}' order by time desc LIMIT 1
                """
            self.database.load_influxdb(database = "account_data")
            ret = self.database.influx_clt.query(a)
            self.database.influx_clt.close()
            data = pd.DataFrame(ret.get_points())
            need_time = data.time.values[-1]
            a = f"""
                select origin, symbol, time from {dataname} where username = '{self.username}' and client = '{self.client}' and time = '{need_time}'
                """
            self.database.load_influxdb(database = "account_data")
            ret = self.database.influx_clt.query(a)
            self.database.influx_clt.close()
            data = pd.DataFrame(ret.get_points())
            upnl = {}
            upnlUsd = {}
            for i in data.index:
                coin = data.loc[i, "symbol"].lower()
                origin_data = eval(data.loc[i, "origin"])
                upnl[coin] = eval(origin_data["upl"])
                price = eval(origin_data["eqUsd"]) / eval(origin_data["eq"])
                upnlUsd[coin] = upnl[coin] * price
        else:
            upnl = {}
            upnlUsd = {}
        self.upnl = upnl
        self.upnlUsd = upnlUsd
        return upnl, upnlUsd
    
    def get_mgnRatio(self):
        exchanges = set([self.exchange_master, self.exchange_slave])
        mr = {}
        for exchange in exchanges:
            if exchange in ["ok", "okex", "okexv5", "okx", "o"]:
                exchange_unified = "okexv5"
            elif exchange in ["binance", "gate"]:
                exchange_unified = exchange
            else:
                print(f"get_mgnRatio: {exchange} has no margin ratio data")
                return 
            
            data = pd.DataFrame()
            num = 0
            while len(data) == 0:
                dataname = "margin_ratio"
                a = f"""
                select mr from {dataname} where username = '{self.username}' and client = '{self.client}' and 
                exchange = '{exchange_unified}' and time> now() - 3m LIMIT 1
                """
                self.database.load_influxdb(database = "account_data")
                ret = self.database.influx_clt.query(a)
                self.database.influx_clt.close()
                data = pd.DataFrame(ret.get_points())
                if len(data) == 0 :
                    num += 1
                    if num < 10:
                        time.sleep(5)
                    else:
                        data.loc[0, "mr"] = np.nan
                        mr[exchange] = np.nan
                        break
            mr[exchange] = data.mr.values[0]
        self.mr = mr.copy()
        
    def get_now_parameter(self):
        mongo_clt = MongoClient(self.mongon_url)
        a = mongo_clt["Strategy_deploy"][self.client].find({"_id": self.deploy_id})
        data = pd.DataFrame(a)
        data = data[data["_id"] == self.deploy_id].copy()
        data.index = range(len(data))
        self.now_parameter = data.copy()
        return data
    
    def get_all_deploys(self):
        mongo_clt = MongoClient(self.mongon_url)
        collections = mongo_clt["Strategy_orch"].list_collection_names()
        collections.remove('test')
        deploy_ids = []
        for key in collections:
            a = mongo_clt["Strategy_orch"][key].find()
            data = pd.DataFrame(a)
            data = data[(data["orch"]) & (data["version"] != "0") & (data["version"] != None) & (data["version"] != "0")].copy()
            deploy_ids += list(data["_id"].values)
        deploy_ids.sort()
        return deploy_ids
    def get_history_parameter(self):
        mongo_clt = MongoClient(self.mongon_url)
        a = mongo_clt["History_Deploy"][self.client].find({"@template": self.deploy_id})
        data = pd.DataFrame(a)
        data = data[data["@template"] == self.deploy_id].copy()
        data.index = range(len(data))
        data["timestamp"] = data["_comments"].apply(lambda x: x["timestamp"])
        data["dt"] = data["timestamp"].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
        self.history_parameter = data.copy()
        return data
    def unified_exchange_name(self, exchange):
        if exchange in ["okx", "okex", "okexv5", "okex5", "ok", "o"]:
            exchange = "okex"
        elif exchange in ["binance", "b"]:
            exchange = "binance"
        elif exchange in ["gate", "gateio", "g"]:
            exchange = "gate"
        elif exchange in ["bybit", "by"]:
            exchange = "bybit"
        elif exchange in ["ftx", "f"]:
            exchange = "ftx"
        else:
            print(f"{self.parameter_name} exchange name is not supported")
        return exchange
    
    def load_suffix_json(self):
        path = self.script_path + "/config"
        with open(f"{path}/unified_suffix.json", "r") as f:
            suffix_json = json.load(f)
        self.suffix_json = suffix_json
    
    def unified_suffix(self, suffix):
        suffix = suffix.replace("_", "-")
        self.load_suffix_json() if not hasattr(self, "suffix_json") else None
        if suffix.split("-")[-1].isnumeric():
            suffix = suffix.replace(suffix.split("-")[-1], "future")
        for key, value in self.suffix_json.items():
            if suffix in value:
                suffix = key
                break
        return suffix
    
    def get_now_position(self, timestamp = "10m"):
        #master, slave : "usdt-swap", "usd-swap", "spot"
        data = pd.DataFrame()
        if self.client == self.slave_client and self.username == self.slave_username:
            a = f"""
            select ex_field, time, exchange, long, long_open_price, settlement, short, short_open_price, pair from position where client = '{self.client}' and username = '{self.username}' and time > now() - {timestamp} and (long >0 or short >0) group by pair, ex_field, exchange ORDER BY time DESC LIMIT 1
            """
            result = self.database._send_influx_query(a, database = "account_data")
            for key in result.keys():
                df = pd.DataFrame(result[key])
                data = pd.concat([data, df])
            data.index = range(len(data))
        else:
            a = f"""
            select ex_field, time, exchange, long, long_open_price, settlement, short, short_open_price, pair from position where client = '{self.client}' and username = '{self.username}' and time > now() - {timestamp} and (long >0 or short >0) group by pair, ex_field, exchange ORDER BY time DESC LIMIT 1
            """
            result = self.database._send_influx_query(a, database = "account_data")
            for key in result.keys():
                df = pd.DataFrame(result[key])
                data = pd.concat([data, df])
            a = f"""
            select ex_field, time, exchange, long, long_open_price, settlement, short, short_open_price, pair from position where client = '{self.slave_client}' and username = '{self.slave_username}' and time > now() - {timestamp} and (long >0 or short >0) group by pair, ex_field, exchange ORDER BY time DESC LIMIT 1
            """
            result = self.database._send_influx_query(a, database = "account_data")
            for key in result.keys():
                df = pd.DataFrame(result[key])
                data = pd.concat([data, df])
            data.index = range(len(data))
        if len(data.columns) > 0:
            data = data[(data["long"] >0) | (data["short"] >0) ].copy()
            data["dt"] = data["time"].apply(lambda x: datetime.datetime.strptime(x[:19],'%Y-%m-%dT%H:%M:%S') + datetime.timedelta(hours = 8))
        else:
            result = pd.DataFrame(columns = ["dt", "side", "master_ex",
                                        "master_open_price", "master_number","master_MV","slave_ex",
                                        "slave_open_price", "slave_number","slave_MV"])
            self.now_position = result.copy()
            return result
        for i in data.index:
            if data.loc[i, "ex_field"] == "swap" and "swap" not in data.loc[i, "pair"]:
                data.loc[i, "pair"] = data.loc[i, "pair"] + "-swap"
        data["coin"] = data["pair"].apply(lambda x : x.split("-")[0])
        for i in data.index:
            suffix = self.unified_suffix(data.loc[i, "pair"][len(data.loc[i, "coin"]):])
            data.loc[i, "kind"] = self.unified_exchange_name(data.loc[i, "exchange"]) + suffix
        data = data[(data["kind"] == self.kind_master) | (data["kind"] == self.kind_slave)].copy()
        data["symbol"] = data["coin"] + "-" + data["kind"]
        df = pd.DataFrame(columns = data.columns)
        symbols = data.symbol.unique()
        for symbol in symbols:
            d = data[data["symbol"] == symbol].copy()
            if "swap" in symbol:
                d = d[d["ex_field"] == "swap"].copy()
            elif "future" in symbol:
                d = d[d["ex_field"] == "futures"].copy()
            else:
                d = d[d["ex_field"] == "spot"].copy()
            n = len(df)
            if len(d) > 0:
                df.loc[n] = d.iloc[-1]
        data = df.copy()
        coins = list(data["coin"].unique())
        if "" in coins:
            coins.remove("")
            location = int(data[data["coin"] == ""].index.values)
            data.drop(location, axis = 0 ,inplace = True)
        self.origin_position = data.copy()
        result = pd.DataFrame(columns = ["dt", "side", "master_ex",
                                         "master_open_price", "master_number","master_MV","slave_ex",
                                        "slave_open_price", "slave_number","slave_MV"], index = coins)
        for i in data.index:
            if data.loc[i, "long"] >0 or data.loc[i, "short"] > 0:
                coin = data.loc[i, "coin"]
                exchange = self.unified_exchange_name(data.loc[i, "exchange"])
                if data.loc[i, "kind"] == self.kind_master:
                    result.loc[coin, "dt"] = data.loc[i, "dt"]
                    name = "master"
                    if data.loc[i, "short"] >0 :
                        result.loc[coin, "side"] = "short"
                    elif data.loc[i, "long"] >0 :
                        result.loc[coin, "side"] = "long"
                elif data.loc[i, "kind"] == self.kind_slave:
                    name = "slave"
                if data.loc[i, "short"] > 0:
                    result.loc[coin, name + "_ex"] = exchange
                    result.loc[coin ,name + "_open_price"] = data.loc[i, "short_open_price"]
                    result.loc[coin ,name + "_number"] = data.loc[i, "short"]
                else:
                    result.loc[coin, name + "_ex"] = exchange
                    result.loc[coin ,name + "_open_price"] = data.loc[i, "long_open_price"]
                    result.loc[coin ,name + "_number"] = data.loc[i, "long"]
                if ("usd-swap" in data.loc[i, "kind"] or "usd-future" in data.loc[i, "kind"]) and "busd-swap" not in data.loc[i, "kind"]:
                    result.loc[coin, name + "_MV"] = result.loc[coin, name + "_number"] * self.contractsize.loc[coin.upper(), exchange + data.loc[i, "kind"].replace(exchange, "")]
                else:
                    try:
                        if self.folder == "dt":
                            price = result.loc[coin ,name + "_open_price"]
                        else:
                            price = self.get_coin_price(coin = coin.lower(), kind = self.master)
                    except:
                        price = np.nan
                        pass
                    if result.loc[coin, name + "_number"] == 0:
                        result.loc[coin, name + "_MV"] = 0
                    else:
                        result.loc[coin, name + "_MV"] = result.loc[coin, name + "_number"] * price
        if len(result) > 30:
            result = result.dropna()
        result["diff"] = result["master_MV"] - result["slave_MV"]
        self.now_position = result.copy()
        return result
    
    def get_dates(self):
        #start ,end : datetime.date
        #dates : string list
        dates = []
        start = self.start.date() + datetime.timedelta(days = -1)
        end = self.end.date()
        i = start
        while i <= end:
            dates.append(str(i))
            i = i + datetime.timedelta(days = 1)
        return dates
    
    def get_account_data(self):
        self.AccountData = AccountData(username = self.username,
                                        client = self.client,
                                        parameter_name= self.parameter_name,
                                        master = self.master,
                                        slave = self.slave,
                                        principal_currency= self.principal_currency,
                                        strategy= self.strategy,
                                        deploy_id= self.deploy_id)
    
    def run_pnl(self, start, end, play = False, log_time = False):
        self.get_account_data() if not hasattr(self, "AccountData") else None
        self.AccountData.run_pnl(start, end, play = play, log_time= log_time)
        self.orders = self.AccountData.orders
        self.trade_data = self.AccountData.trade_data
        self.tpnl = self.AccountData.tpnl
        self.ledgers = self.AccountData.ledgers
        self.ledgers_fpnl = self.AccountData.ledgers_fpnl
        self.fpnl = self.AccountData.fpnl
        self.second_pnl = self.AccountData.second_pnl
        self.third_pnl = self.AccountData.third_pnl
        self.pnl = self.AccountData.adjEq
        self.total_pnl = self.AccountData.total_pnl
