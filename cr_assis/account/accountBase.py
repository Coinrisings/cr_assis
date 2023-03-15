import pandas as pd
import numpy as np
from cr_assis.draw import draw_ssh
import datetime, time, os, yaml, json
from pymongo import MongoClient
from cr_assis.connect.connectData import ConnectData
from pathlib import Path
from bokeh.plotting import show
from bokeh.models.widgets import Panel, Tabs

class AccountBase(object):
    
    def __init__(self,  deploy_id: str, strategy = "funding") -> None:
        self.deploy_id = deploy_id
        self.strategy = strategy
        self.script_path = str(Path( __file__ ).parent.parent.absolute())
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
        """initilize account's info by deploy_id"""
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
        self.secret_master = data.loc[0, "secret_master"]
        self.secret_slave = data.loc[0, "secret_slave"]
        path1 = self.secret_master.replace("/", "__").replace(":", "_")
        path2 = self.secret_slave.replace("/", "__").replace(":", "_")
        paths = [path1, path2]
        self.path_orders = paths
        self.path_ledgers = paths
    
    def get_folder(self):
        """ parameter folder in GitHub """
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
            set: set of name, {'spread_orderbook_binance_spot_audio_usdt__orderbook_binance_swap_audio_usdt_swap__reverse',
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
    
    def get_coins_price(self, coins: list, kind = "", delivery = "") -> dict:
        """get coins price from redis

        Args:
            coin (str): the list of coins name, like ["btc", "eth"]
            kind (str, optional): exchange + contract type, for example "okex_spot", "okex_usdt_swap". Defaults to "".
            delivery (str, optional):delivery data, only for future. Defaults to "".
        """
        if kind == "":
            kind = self.exchange_master + "_spot"
        kind = kind.replace("-", "_")
        exchange = kind.split("_")[0]
        x = kind.split("_")[-1]
        if x == "swap":
            suffix = "-" + kind.split("_")[-2] + "-" + kind.split("_")[-1]
        elif x in ["spot", "usdt"]:
            suffix = "-usdt"
        elif x == "future":
            if delivery == "":
                delivery = self.get_quarter()
            suffix = "-" + kind.split("_")[-2] + "-" + delivery
        else:
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
            pass
        self.database.load_redis()
        r = self.database.redis_clt
        coin_price = {}
        for coin in coins:
            key = f"{exchange}/{coin}{suffix}"
            key = bytes(key, encoding = "utf8")
            data = r.hgetall(key)
            if b'bid0_price' in data.keys():
                price = eval(data[b'bid0_price'])
            else:
                price = np.nan
            coin_price[coin] = price
        return coin_price
    
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
    
    def get_mean_equity(self, the_time = "now()", interval = "1d") -> float:
        equity = np.nan
        ccy = self.principal_currency.lower()
        a = f"""
        select {ccy} as equity, balance_id from balance_v2 where username = '{self.username}' and client = '{self.client}' and time >= {the_time} - {interval} and time <= {the_time}
        """
        ret = self.database._send_influx_query(a, database = "account_data")
        if "balance_id" in ret.columns:
            ret.dropna(subset=["balance_id"], inplace = True)
        if len(ret) > 0:
            equity = np.mean(ret["equity"])
        return equity
        
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
    
    def _send_complex_query(self, sql: str, db = "account_data") -> pd.DataFrame:
        """send complex query to influx, convert dict to pd.DataFrame

        Args:
            sql (str): sql query
            db (str, optional): database name. Defaults to "account_data".
            en (str, optional): os.environ. Defaults to "INFLUX_URI".

        Returns:
            pd.DataFrame: raw position data in influx
        """
        data = pd.DataFrame()
        result = self.database._send_influx_query(sql, database = db, is_dataFrame= False)
        for key in result.keys():
            df = pd.DataFrame(result[key])
            data = pd.concat([data, df])
        return data
    
    def get_influx_position(self, timestamp: str, the_time: str) -> pd.DataFrame:
        a = f"""
            select ex_field, time, exchange, long, long_open_price, settlement, short, short_open_price, pair from position where client = '{self.client}' and username = '{self.username}' and time > {the_time} - {timestamp} and time < {the_time} and (long >0 or short >0) group by pair, ex_field, exchange ORDER BY time DESC LIMIT 1
            """
        data = self._send_complex_query(sql = a)
        if self.client != self.slave_client or self.username != self.slave_username:
            a = f"""
            select ex_field, time, exchange, long, long_open_price, settlement, short, short_open_price, pair from position where client = '{self.slave_client}' and username = '{self.slave_username}' and time > {the_time} - {timestamp} and time < {the_time} and (long >0 or short >0) group by pair, ex_field, exchange ORDER BY time DESC LIMIT 1
            """
            df = self._send_complex_query(sql = a)
            data = pd.concat([data, df])
        data.index = range(len(data))
        return data
    
    def get_now_position(self, timestamp = "10m", the_time = "now()"):
        """ master, slave : "usdt-swap", "usd-swap", "spot" """
        #master, slave : "usdt-swap", "usd-swap", "spot"
        if the_time != "now()" and "'" not in the_time:
            the_time = f"'{the_time}'"
        data = self.get_influx_position(timestamp = timestamp, the_time=the_time)
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
        data.sort_values(by = "time", inplace = True)
        data.drop_duplicates(subset = ["symbol"], keep = "last", ignore_index= True)
        data["name"] = data["kind"].apply(lambda x: "master" if x == self.kind_master else "slave")
        data["number"] = data["long"] - data["short"]
        data["side"] = data["number"].apply(lambda x: "long" if x >=0 else "short")
        data["open_price"] = data["long_open_price"] + data["short_open_price"]
        
        if self.contract_master in ["-usd-swap", "-usd-future"]:
            data["master_contractsize"] = data["coin"].apply(lambda x: self.contractsize.loc[x.upper(), self.kind_master])
        if self.contract_slave in ["-usd-swap", "-usd-future"]:
            data["slave_contractsize"] = data["coin"].apply(lambda x: self.contractsize.loc[x.upper(), self.kind_slave])
        if self.folder != "dt":
            coin_price = self.get_coins_price(coins = list(data["coin"].unique()))
            data["coin_price"] = data["coin"].apply(lambda x: self.get_coin_price(coin = x.lower()) if x not in coin_price.keys() else coin_price[x])
        self.origin_position = data.copy()
        master = pd.DataFrame(columns = ["dt", "side", "master_ex",
                                        "master_open_price", "master_number","master_MV"])
        origin_master = data[data["kind"] == self.kind_master].copy()
        master["dt"] = origin_master["dt"]
        master["coin"] = origin_master["coin"]
        master["side"] = origin_master["side"]
        master["master_ex"] = origin_master["exchange"].apply(lambda x: self.unified_exchange_name(x))
        master["master_open_price"] = origin_master["open_price"]
        master["master_number"] = abs(origin_master["number"])
        if self.contract_master in ["-usd-swap", "-usd-future"]:
            master["master_MV"] = abs(origin_master["number"] * origin_master["master_contractsize"])
        elif self.folder == "dt":
            master["master_MV"] = master["master_open_price"] * master["master_number"]
        else:
            master["master_MV"] = master["master_number"] * origin_master["coin_price"]
        
        slave = pd.DataFrame(columns = ["slave_ex", "slave_open_price", "slave_number","slave_MV"])
        origin_slave = data[data["kind"] == self.kind_slave].copy()
        slave["coin"] = origin_slave["coin"]
        slave["slave_ex"] = origin_slave["exchange"].apply(lambda x: self.unified_exchange_name(x))
        slave["slave_open_price"] = origin_slave["open_price"]
        slave["slave_number"] = abs(origin_slave["number"])
        if self.contract_slave in ["-usd-swap", "-usd-future"]:
            slave["slave_MV"] = abs(origin_slave["number"] * origin_slave["slave_contractsize"])
        elif self.folder == "dt":
            slave["slave_MV"] = slave["slave_open_price"] * slave["slave_number"]
        else:
            slave["slave_MV"] = slave["slave_number"] * origin_slave["coin_price"]
        
        result = pd.merge(master, slave, on = "coin", how = "outer")
        result.set_index("coin", inplace = True)
        result["diff"] = result["master_MV"] - result["slave_MV"]
        self.now_position = result.copy()
        return result
    
    def get_dates(self):
        dates = []
        start = self.start.date() + datetime.timedelta(days = -1)
        end = self.end.date()
        i = start
        while i <= end:
            dates.append(str(i))
            i = i + datetime.timedelta(days = 1)
        return dates
    
    def get_orders_data(self, log = False):
        # filename is UTC
        start = self.start
        end = self.end
        username = self.username
        client = self.client
        transfer = {}
        transfer["_spot"] = "spot"
        transfer["-usdt"] = "spot"
        transfer["_usdt_swap"] = "swap_usdt"
        transfer["_usdc_swap"] = "swap_usdt"
        transfer["_usd_swap"] = "swap_usd"
        transfer["-usdt-swap"] = "swap_usdt"
        transfer["-usdc-swap"] = "swap_usdt"
        transfer["-usd-swap"] = "swap_usd"
        transfer["_busd_swap"] = "swap_busd"
        transfer["-busd-swap"] = "swap_busd"
        transfer["_usd_future"] = "futures_usd"
        transfer["-usd-future"] = "futures_usd"
        transfer["_usdt_future"] = "futures_usdt"
        transfer["-usdt-future"] = "futures_usdt"
        path = "/mnt/efs/fs1/data_center/orders/"
        contracts = []
        for name in self.path_orders:
            if transfer[self.contract_master] in name or transfer[self.contract_slave] in name:
                contracts.append(path + name)
        self.contracts = contracts.copy()
        start_date = start.date()
        end_date = end.date()
        dates = self.get_dates()
        orders = {}
        
        for contract in contracts:
            data = pd.DataFrame()
            for date in dates:
                try:
                    df = pd.read_csv(contract + '/' + date + '.csv')
                except:
                    if log:
                        print(f"{self.parameter_name}  {contract}  {date} orders data NA")
                    continue
                df["dt"] = df["update_iso"].apply(lambda x: datetime.datetime.strptime(x[:19],'%Y-%m-%dT%H:%M:%S') + datetime.timedelta(hours = 8))
                df1 = df[df["dt"]<= end]
                df2 = df1[df1["dt"] >= start]
                data = pd.concat([data, df2])
            data.index = range(len(data))
            a = contract.split("/")[-1]
            a = a.split("@")[-1]
            orders[a] = data.copy()
        self.orders = orders.copy()
        return orders
    
    def get_spot_orders(self, log = False):
        path = "/mnt/efs/fs1/data_center/orders/"
        start = self.start
        end = self.end
        start_date = start.date()
        end_date = end.date()
        dates = self.get_dates()
        for name in self.path_orders:
            if name.split("_")[-1] == "spot":
                path = path + name + "/"
                break
        
        data = pd.DataFrame()
        for date in dates:
            try:
                df = pd.read_csv(path + date + '.csv')
            except:
                continue
            df["dt"] = df["update_iso"].apply(lambda x: datetime.datetime.strptime(x[:19],'%Y-%m-%dT%H:%M:%S') + datetime.timedelta(hours = 8))
            df1 = df[df["dt"]<= end]
            df2 = df1[df1["dt"] >= start]
            data = pd.concat([data, df2])
        return data
    def get_klines_data(self, start, end, exchange, coins, contract, log = False):
        #coins : string list ["ADA", "ATOM" ... ]
        path = "/mnt/efs/fs1/data_center/klines/"
        dates = self.get_dates()
        exchange = self.unified_exchange_name(exchange)
        if exchange in ["okex", "okx", "okex5", "okexv5"]:
            if contract in ["swap_usd", "swap_usdt", "usdt_swap", "usd_swap"]:
                filepath = "okex5-swap/"
            elif contract == "spot":
                filepath = "okex5-spot/"
            else:
                print(f"contract error")
                return 
        elif exchange in ["gate", "gateio"]:
            if contract in ["swap_usdt", "usdt_swap", "swap_usd", "usd_swap"]:
                filepath = "gate-swap/"
            elif contract == "spot":
                filepath = "gateio-spot/"
            else:
                print(f"contract error")
                return 
        elif exchange == "bybit":
            if contract in ["swap_usdt", "usdt_swap", "swap_usd", "usd_swap"]:
                filepath = "bybit-swap/"
            elif contract == "spot":
                filepath = "bybit-spot/"
            else:
                print(f"contract error")
                return 
        elif exchange == "ftx":
            if contract in ["swap_usdt", "usdt_swap", "swap_usd", "usd_swap"]:
                filepath = "ftx-swap/"
            elif contract == "spot":
                filepath = "ftx-spot/"
            else:
                print(f"contract error")
                return 
        elif exchange == "binance":
            if contract in ["swap_usdt", "usdt_swap"]:
                filepath = "binance-swap/"
            elif contract in ["swap_usd", "usd_swap"]:
                filepath = "binancecoin-swap/"
            elif contract == "spot":
                filepath = "binance-spot/"
            else:
                print(f"contract error")
                return 
        else:
            print("exchange error")
            return 
        klines = {}

        for coin in coins:
            coin = coin.upper()
            if contract in ["swap_usdt", "usdt_swap", "spot"]:
                filename = coin + "-USDT"
            elif contract in ["swap_usd", "usd_swap"]:
                filename = coin + "-USD"
            else:
                print("contract error")
                return 
            data = pd.DataFrame()
            for date in dates:
                df = pd.DataFrame()
                try:
                    df = pd.read_csv(path + filepath + filename + "/" + date + ".csv")
                except:
                    if log:
                        print(f"{exchange} {coin} {contract} {date} klines data NA")
                    pass
                if "time" in df.columns:
                    df["dt"] = df["time"].apply(lambda x: datetime.datetime.strptime(x[:19], "%Y-%m-%d %H:%M:%S") + datetime.timedelta(hours = 8))
                    data = pd.concat([data, df])
            if exchange in ["gate", "gateio","ftx"]:
                data["volume_U"] = data["volume"]
            else:
                data["volume_U"] = data["volume"] * data["close"]
            if "dt" in data.columns:
                data = data[data["dt"]>= start + datetime.timedelta(minutes = -10)].copy()
                data = data[data["dt"]<= end + datetime.timedelta(minutes = 10)].copy()
            data.index = range(len(data))
            klines[coin] = data.copy()
        self.klines = klines.copy()
        return klines
    
    def handle_orders_data(self, play = True):
        orders = self.orders
        contractsize = self.contractsize
        raw = pd.DataFrame()
        names = ["dt", "pair", "avg_price", "cum_deal_base","side", "exchange", "field",  "status", "settlement", "market_oid"]
        for contract in orders.keys():
            raw = pd.concat([raw, orders[contract]])
        raw = raw.drop_duplicates(subset = ["market_oid"])
        raw.index = range(len(raw))
        data = pd.DataFrame(columns = ["UTC", "dt", "pair", "coin", "avg_price", "cum_deal_base",
                                        "side","turnover","status","exchange", "field", "market_oid"])
        if len(raw) != 0:
            for i in raw.index:
                data.loc[i, "UTC"] = raw.loc[i, "update_iso"]
                for name in names:
                    data.loc[i, name] = raw.loc[i, name]
                    if name == "cum_deal_base":
                        data.loc[i, name] = abs(raw.loc[i, name])
                if type(data.loc[i, "side"]) != type("a"):
                    if np.isnan(data.loc[i, "side"]):
                        side = eval(raw.loc[i, "raw"])["side"]
                        if side == "closeshort":
                            side = "openlong"
                        elif side == "closelong":
                            side = "openshort"
                        data.loc[i, "side"] = side
                if data.loc[i, "field"] == "swap" and "ok" in data.loc[i, "exchange"]:
                    coin = data.loc[i, "pair"].split("-")[0].upper()
                    kind = data.loc[i, "pair"].split("-")[1]
                    exchange = self.unified_exchange_name(data.loc[i, "exchange"])
                    x = exchange + "-" + kind + "-swap"
                    data.loc[i, "cum_deal_base"] = contractsize.loc[coin, x] * data.loc[i, "cum_deal_base"]
                if self.combo != "okx_usdt_swap-okx_usd_swap":
                    if data.loc[i, "side"] in ["openshort", "closelong"]:
                        if data.loc[i, "field"] == "swap" and data.loc[i, "pair"].split("-")[1] == "usd":
                            data.loc[i, "turnover"] = data.loc[i, "cum_deal_base"]
                        else:
                            data.loc[i, "turnover"] = data.loc[i, "avg_price"] * data.loc[i, "cum_deal_base"]
                    elif data.loc[i, "side"] in ["openlong", "closeshort"]:
                        if data.loc[i, "field"] == "swap" and data.loc[i, "pair"].split("-")[1] == "usd":
                            data.loc[i, "turnover"] = - data.loc[i, "cum_deal_base"]
                        else:
                            data.loc[i, "turnover"] = - data.loc[i, "avg_price"] * data.loc[i, "cum_deal_base"]
                else:
                    kind = data.loc[i, "pair"].split("-")[1]
                    if kind in ["usd", "USD"]:
                        if data.loc[i, "side"] == "openshort":
                            data.loc[i, "turnover"] = data.loc[i, "cum_deal_base"]
                        else:
                            data.loc[i, "turnover"] = - data.loc[i, "cum_deal_base"]
                    else:
                        if eval(eval(raw.loc[i, "raw"])["pnl"]) == 0:
                            if data.loc[i, "side"] == "openshort":
                                data.loc[i, "turnover"] = data.loc[i, "cum_deal_base"] * data.loc[i, "avg_price"]
                            else:
                                data.loc[i, "turnover"] = - data.loc[i, "cum_deal_base"] * data.loc[i, "avg_price"]
                        else:
                            pair = data.loc[i, "pair"]
                            timestamp = raw.loc[i, "update_iso"]
                            a = f"""
                            select long_open_price, short_open_price from position where client = '{self.client}' and 
                            username = '{self.username}' and pair = '{pair}' and time <= '{timestamp}' order by time desc LIMIT 1
                            """
                            df = self.database._send_influx_query(a, database = "account_data", is_dataFrame=True)
                            price = max([df.loc[0, "long_open_price"], df.loc[0, "short_open_price"]])
                            if data.loc[i, "side"] == "openshort":
                                data.loc[i, "turnover"] = data.loc[i, "cum_deal_base"] * price
                            else:
                                data.loc[i, "turnover"] = - data.loc[i, "cum_deal_base"] * price
            data["coin"] = data["pair"].apply(lambda x: x.split("-")[0].upper())
            data = data.sort_values(by = "dt")
            data.index = range(len(data))
            result = pd.DataFrame(columns = ["turnover"])
            result["turnover"] = data["turnover"]
            result = result.cumsum()
            result["dt"] = data["dt"]
            ps = {}
            ts = {}
            title_name = f"{self.parameter_name}: {self.start} to {self.end}"
            p0 = draw_ssh.line(result, x_axis_type = "linear", play = False, title = title_name, tips=[('x', '$x{0}'), ('value','$y{0.0000}'),('name','$name'), ('time', '@dt{%Y-%m-%d %H:%M:%S}')], formatters={"@x": "printf", "@dt": "datetime"}, tags = ["dt"])
            tab0 = Panel(child=p0, title="Overview")
            coins = list(data.coin.unique())
            for i in range(len(coins)):
                a = data[data["coin"] == coins[i]].copy()
                result = pd.DataFrame(columns = ["turnover", "cum_deal_base"])
                for j in a.index:
                    if a.loc[j, "side"] in ["openlong", "closeshort"]:
                        a.loc[j, "cum_deal_base"] = -a.loc[j, "cum_deal_base"]
                result["turnover"] = a["turnover"]
                result["cum_deal_base"] = a["cum_deal_base"]
                result.index = range(len(result))
                result = result.cumsum()
                result = result.fillna(0)
                result["dt"] = data["dt"]
                ps[i] = draw_ssh.line_doubleY(result, right_columns = ["cum_deal_base"],x_axis_type = "linear", play = False, title = title_name, tips=[('x', '$x{0}'), ('value','$y{0.0000}'),('name','$name'), ('time', '@dt{%Y-%m-%d %H:%M:%S}')], formatters={"@x": "printf", "@dt": "datetime"}, tags = ["dt"])
                ts[i] = Panel(child = ps[i], title=coins[i])
            tabs = []
            tabs.append(tab0)
            for i in ts.keys():
                tabs.append(ts[i])
            t = Tabs(tabs= tabs)
            if play:
                show(t)
        self.trade_data = data.copy()
        return data
    
    def get_tpnl(self):
        trade_data = self.trade_data
        coins = list(trade_data.coin.unique())
        data = pd.DataFrame(columns = ["total"], index = coins)
        for col in data.columns:
            data[col] = 0
        for coin in coins:
            a = trade_data
            data.loc[coin, "total"] = sum(a[a['coin'] == coin]["turnover"].values)
        self.tpnl = data.copy()
        return data
    
    def get_ledgers(self, log = False):
        start = self.start
        end = self.end
        username = self.username 
        client = self.client
        path = "/mnt/efs/fs1/data_center/ledgers/"
        kinds = []
        for name in self.path_ledgers:
            kinds.append(path + name)
        dates = self.get_dates()
        data = pd.DataFrame()
        for date in dates:
            if len(kinds) > 0 :
                for kind in kinds:
                    try:
                        df = pd.read_csv(kind + "/" + date + '.csv')
                    except:
                        if log:
                            print(f"{username} {client} {date} {kind} ledgers NA")
                        continue
                    data = pd.concat([data, df])
        data = data.drop_duplicates()
        if len(data) == 0:
            fpnl = pd.DataFrame(columns = ["total"])
            self.fpnl = fpnl.copy()
            self.ledgers = data.copy()
            return data
        data["dt"] = data["update_iso"].apply(lambda x: datetime.datetime(int(x[0:4]), int(x[5:7]),int(x[8:10]),int(x[11:13])) + datetime.timedelta(hours = 8))
        data = data[data["dt"] >= start].copy()
        data = data[data["dt"] <= end].copy()
        data.index = range(len(data))
        if len(data) >0 :
            for i in data.index:
                if type(data.loc[i, "symbol"]) != type("a") and np.isnan(data.loc[i, "symbol"]):
                    if data.loc[i, "type"] == "funding_fee" and "usdt" in data.loc[i, "pair"].lower():
                        data.loc[i, "symbol"] = "usdt"
        else:
            fpnl = pd.DataFrame(columns = ["total"])
            self.fpnl = fpnl.copy()
        self.ledgers = data.copy()
        return data
    def get_fpnl(self, log = False):
        data = self.ledgers.copy()
        pairs = list(data.pair.unique())
        coins = set()
        stable_coins = ["USDT", "USD", "USDC", "USDK", "USDP", "DAI", "BUSD"]
        if np.nan in pairs:
            pairs.remove(np.nan)
        
        for pair in pairs:
            if type(pair) == type("a"):
                coin = pair.split("-")[0].upper()
                coins.add(coin)
        coins = list(coins)
        symbols0 = list(data["symbol"].unique())
        symbols = []
        for i in symbols0:
            if type(i) == str:
                symbols.append(i)
        if len(symbols) == 0:
            symbols.append("USDT")
        for i in range(len(symbols)):
            symbols[i] = symbols[i].upper()
        symbols = list(set(symbols))
        for symbol in stable_coins:
            if symbol in symbols:
                symbols.remove(symbol)
        if len(symbols)>0:
            klines = self.get_klines_data(start = self.start, end = self.end, exchange = self.exchange_master,coins = symbols, contract = "spot")
        if "interest" in data.type.values:
            fpnl = pd.DataFrame(columns = ["funding_fee", "interest", "total"])
        else:
            fpnl = pd.DataFrame(columns = ["total"])
        if len(data) >0 :
            for i in data.index:
                if type(data.loc[i, "symbol"]) == str:
                    symbol = data.loc[i, "symbol"].upper()
                else:
                    symbol = "USDT"
                dt = data.loc[i, "dt"] + datetime.timedelta(seconds = - data.loc[i, "dt"].second)
                if symbol in stable_coins:
                    price = 1
                else:
                    if "dt" in klines[symbol].columns and dt in klines[symbol]["dt"].values:
                        price = float(klines[symbol][klines[symbol]["dt"] == dt].close.values)
                    else:
                        price = np.nan
                data.loc[i, "price"] = price
                amount = data.loc[i, "amount"]
                data.loc[i, "balance_change_U"] = amount * data.loc[i, "price"]
            for coin in coins:
                names = []
                for stable in stable_coins:
                    names.append(f"{coin.upper()}-{stable}-SWAP")
                if "interest" in data.type.values:
                    interest = 0
                    df = data[((data["symbol"] == coin) | (data["symbol"] == coin.upper()))].copy()
                    interest += sum(df[df["type"] == "interest"]["balance_change_U"].values)
                    fpnl.loc[coin.upper(), "interest"] = interest
                    funding_fee = 0
                    for name in names:
                        df = data[((data["pair"] == name) | (data["pair"] == name.lower()))].copy()
                        funding_fee += sum(df[df["type"] == "funding_fee"]["balance_change_U"].values)
                    fpnl.loc[coin.upper(), "funding_fee"] = funding_fee
                    fpnl.loc[coin.upper(), "total"] = fpnl.loc[coin.upper(), "interest"] + fpnl.loc[coin.upper(), "funding_fee"]
                else:
                    funding_fee = 0
                    for name in names:
                        df = data[((data["pair"] == name) | (data["pair"] == name.lower()))].copy()
                        funding_fee += sum(df[df["type"] == "funding_fee"]["balance_change_U"].values)
                    fpnl.loc[coin.upper(), "total"] = funding_fee
        self.ledgers_fpnl = data.copy()
        self.fpnl = fpnl.copy()
        return fpnl
    
    def get_pnl(self, play = True, number = 34):
        tpnl = self.tpnl
        fpnl = self.fpnl
        coins = list(set(list(tpnl.index) + list(fpnl.index)))
        pnl = pd.DataFrame(index = coins, columns = ["tpnl", "fpnl", "total"])
        total_pnl = pd.DataFrame(columns = ["tpnl", "fpnl", "total"])
        for coin in coins:
            if coin in tpnl.index:
                pnl.loc[coin, "tpnl"] = tpnl.loc[coin, "total"]
            else:
                pnl.loc[coin, "tpnl"] = 0
            if coin in fpnl.index:
                pnl.loc[coin, "fpnl"] = fpnl.loc[coin, "total"]
            else:
                pnl.loc[coin, "fpnl"] = 0
            pnl.loc[coin, "total"] = pnl.loc[coin, "tpnl"] + pnl.loc[coin, "fpnl"]
        if "total" in pnl.columns:
            pnl = pnl.sort_values(by = "total", ascending = False)
        for col in pnl.columns:
            total_pnl.loc["Total", col] = sum(pnl[col].values)
        if len(pnl) > 0:
            result = pnl.copy()
            if result.shape[0] > number:
                result = result[abs(result["total"]) >= 1]
            if result.shape[0] > number:
                result["abs"] = result["total"].abs()
                result = result.sort_values(by = "abs", ascending = False)
                result = result[:number].sort_values("total", ascending = False)
                result = result.drop("abs", axis = 1)
            result = result.drop("total", axis = 1)
            result = result.fillna("")
            title_name = f"{self.parameter_name}: {self.start} to {self.end}"
            p1 = draw_ssh.bar(result, play = False, title = title_name)
            tab1 = Panel(child=p1, title="Trade And Funding")
            result = tpnl.sort_values(by = "total", ascending = False)
            result = result.fillna("")
            p2 = draw_ssh.bar(result, play = False, title = title_name)
            tab2 = Panel(child=p2, title="Trade PNL")
            result = fpnl.sort_values(by = "total", ascending = False)
            result = result.fillna("")
            if result.shape[0] > number:
                result = result[abs(result["total"]) >= 1]
            if result.shape[0] > number:
                result["abs"] = result["total"].abs()
                result = result.sort_values(by = "abs", ascending = False)
                result = result[:number].sort_values("total", ascending = False)
                result = result.drop("abs", axis = 1)
            p3 = draw_ssh.bar(result, play = False, title = title_name)
            tab3 = Panel(child=p3, title="Funding PNL")
            tabs = Tabs(tabs=[tab1, tab2, tab3])
            if play :
                show(tabs)
        self.pnl = pnl.copy()
        self.total_pnl = total_pnl.copy()
        return pnl, total_pnl
    
    def get_third_pnl(self):
        cash = {}
        price = np.nan
        self.end = datetime.datetime.utcnow() + datetime.timedelta(hours = 8) if not hasattr(self, "end") else self.end
        self.start = self.end + datetime.timedelta(hours=-24) if not hasattr(self, "start") else self.start
        for timestamp in [self.start, self.end]:
            start = timestamp + datetime.timedelta(minutes = -10, hours = -8)
            end = timestamp + datetime.timedelta(hours = -8)
            a = f"""
            select origin from equity_snapshot where client = '{self.client}' and username = '{self.username}' 
            and symbol = '{self.principal_currency.lower()}' and time <= '{end}' and time >= '{start}' LIMIT 100
            """
            df = self.database._send_influx_query(a, database = "account_data", is_dataFrame= True)
            if len(df) > 0:
                raw = eval(df.origin.values[-1])
                cash[timestamp] = eval(raw["eq"]) - eval(raw["upl"])
                if timestamp == self.end:
                    price = eval(raw["eqUsd"]) / eval(raw["eq"])
            else:
                cash[timestamp] = np.nan
        profit = cash[self.end] - cash[self.start]
        third_pnl = {f"{self.principal_currency}":profit, "USDT":profit * price}
        self.cash = cash.copy()
        self.third_pnl = third_pnl.copy()
        return third_pnl
    
    def get_second_pnl(self):
        """get orders data, including swap and spot"""
        orders = self.orders.copy()
        data = pd.DataFrame()
        for key in orders.keys():
            data = pd.concat([data, orders[key]])
        spot_orders = self.get_spot_orders()
        data = pd.concat([data, spot_orders])

        #drop principal coin and usd-swap
        if "pair" in data.columns:
            data["coin"] = data["pair"].apply(lambda x: x.split("-")[0].upper())
            data["not_principal"] = data["coin"].apply(lambda x: True if self.principal_currency.upper() != x else False)
            data = data[data["not_principal"]].copy()
            if "pair" in data.columns:
                data["is_usdt"] = data["pair"].apply(lambda x: True if x.split("-")[1].lower() == "usdt" else False)
                data = data[data["is_usdt"]].copy()
            else:
                second_pnl = pd.DataFrame(columns = ["profit"])
                self.second_pnl = second_pnl.copy()
                return second_pnl
        else:
            second_pnl = pd.DataFrame(columns = ["profit"])
            self.second_pnl = second_pnl.copy()
            return second_pnl

        #calculate profit
        data.index = range(len(data))
        for i in data.index:
            if "swap" in data.loc[i, "pair"]:
                data.loc[i, "profit"] = eval(eval(data.loc[i, "raw"])["pnl"]) + eval(eval(data.loc[i, "raw"])["fee"])
            else:
                if data.loc[i, "side"] in ["openshort", "closelong"]:
                    data.loc[i, "profit"] = data.loc[i, "avg_price"] * data.loc[i, "cum_deal_base"]
                elif data.loc[i, "side"] in ["openlong", "closeshort"]:
                    data.loc[i, "profit"] = - data.loc[i, "avg_price"] * data.loc[i, "cum_deal_base"]
        self.second_orders = data.copy()

        #get ledgers data and drop usdt-swap
        ledgers = self.get_ledgers().copy()
        ledgers = ledgers[ledgers["type"] == "funding_fee"].copy()
        ledgers = ledgers[ledgers["symbol"] == "usdt"].copy()
        ledgers["coin"] = ledgers["pair"].apply(lambda x: x.split("-")[0].upper())
        ledgers = ledgers[ledgers["coin"] != self.principal_currency.upper()].copy()
        ledgers.index = range(len(ledgers))
        self.second_ledgers = ledgers.copy()

        #calculate coin profit
        if "coin" in data.columns:
            coins = list(set(data.coin.unique()) | set(data.coin.unique()))
        else:
            coins = []
        second_pnl = pd.DataFrame(columns = ["profit"], index = coins)
        for coin in coins:
            if coin in data["coin"].values:
                profit = sum(data[data["coin"] == coin].profit.values)
            else:
                profit = 0
            if coin in ledgers.coin.values:
                profit += sum(ledgers[ledgers["coin"] == coin].amount.values)
            second_pnl.loc[coin, "profit"] = profit
        
        #calculate princial currency pnl
        if "third_pnl" not in self.__dict__.keys():
            self.get_third_pnl()
        third_pnl = self.third_pnl
        total_others = sum(second_pnl.profit.values)
        second_pnl.loc[self.principal_currency, "profit"] = third_pnl["USDT"] - total_others
        second_pnl.sort_values(by = "profit", ascending = False, inplace = True)
        self.second_pnl = second_pnl.copy()
        return second_pnl
    
    def run_pnl(self, start, end, play = True, log_time = True):
        self.start = start
        self.end = end
        orders = self.get_orders_data()
        trade_data = self.handle_orders_data(play = play)
        tpnl = self.get_tpnl()
        ledgers = self.get_ledgers()
        if len(ledgers) > 0:
            fpnl = self.get_fpnl()
        pnl, total_pnl = self.get_pnl(play = play)
        if self.combo in ["okx_usdt_swap-okx_usd_swap", "okx_usd_swap-okx_usdt_swap"]:
            third_pnl = self.get_third_pnl()
            second_pnl = self.get_second_pnl()
        if log_time:
            if len(trade_data) > 0:
                trade_data = trade_data.sort_values(by = "dt")
                tpnl_start = trade_data["dt"].values[0]
                tpnl_end = trade_data["dt"].values[-1]
                print(f"tpnl: {tpnl_start}  to {tpnl_end}")
            else:
                print(f"orders data is empty")
            if len(ledgers) > 0:
                ledgers = ledgers.sort_values(by = "dt")
                fpnl_start = ledgers["dt"].values[0]
                fpnl_end = ledgers["dt"].values[-1]
                print(f"fpnl: {fpnl_start}  to {fpnl_end}")
            else:
                print(f"ledgers data is empty")
