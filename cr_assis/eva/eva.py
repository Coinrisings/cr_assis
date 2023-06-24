import datetime, os
import multiprocessing
from concurrent.futures import as_completed,ThreadPoolExecutor
from cr_assis.api.okex.marketApi import MarketAPI
import numpy as np
import pandas as pd
from research.utils import draw_ssh, readData
from bokeh.models.widgets import Panel, Tabs
from bokeh.io import output_notebook
from bokeh.plotting import figure,show

def get_funding_time(start_date, end_date, string = False):
    funding_time = []
    date = start_date
    end = datetime.datetime.now() + datetime.timedelta(hours = 8)
    end = str(end)
    while date <= end_date:
        for i in ["00","08","16"]:
            a = str(date) + " " + i + ":00:00"
            if a <= end:
                if string:
                    a = a.replace(" ", "T") + "Z"
                    funding_time.append(a)
                else:
                    a = datetime.datetime.strptime(a, "%Y-%m-%d %H:%M:%S")
                    funding_time.append(pd.Timestamp(a))
        date = date + datetime.timedelta(days = 1)
    return funding_time
def unified_suffix(suffix):
    if suffix in ["-busd-swap", "_busd_swap", "busd-swap", "busd_swap"]:
        suffix = "-busd-swap"
    elif suffix in ["-usdt-swap", "_usdt_swap", "usdt-swap", "usdt_swap"]:
        suffix = "-usdt-swap"
    elif suffix in ["-usd-swap", "_usd_swap", "usd-swap", "usd_swap"]:
        suffix = "-usd-swap"
    elif suffix in ["-usdt", "_usdt", "usdt", "spot", "-spot", "_spot"]:
        suffix = "-usdt"
    else:
        print(f"{suffix} suffix is not supported: {suffix}")
    return suffix
def get_spreads(master, slave, coin, hours = 24):
    exchange_master = master.split("_")[0]
    exchange_slave = slave.split("_")[0]
    contract_master = master.replace(exchange_master, "")
    contract_slave = slave.replace(exchange_slave, "")
    exchange_master = unified_exchange_name(exchange_master)
    exchange_slave = unified_exchange_name(exchange_slave)
    contract_master = unified_suffix(contract_master)
    contract_slave = unified_suffix(contract_slave)
    kind_master = exchange_master + contract_master
    kind_slave = exchange_slave + contract_slave
    coin = coin.lower()
    contract_master = contract_master.replace("-", "_")
    contract_slave = contract_slave.replace("-", "_")
    kind_master = master.split("_")[-1]
    kind_slave = slave.split("_")[-1]
    if exchange_master in ["okx", "okexv5"]:
        exchange_master = "okex"
    if exchange_slave in ["okx", "okexv5"]:
        exchange_slave = "okex"
    dataname = f'''spread_orderbook_{exchange_master}_{kind_master}_{coin}{contract_master}__orderbook_{exchange_slave}_{kind_slave}_{coin}{contract_slave}'''
    a = f"select time, ask0_spread, bid0_spread from {dataname} where time >= now() - {hours}h"
    spreads_data = readData.read_influx(a, db = "spreads", en = 'INFLUX_MARKET_URI')
    return spreads_data
def get_15d_funding_time():
    end = datetime.datetime.now() + datetime.timedelta(hours = 8)
    start = end + datetime.timedelta(days = -16)
    date = start.date()
    end_date = end.date()
    end = str(end)
    funding_time = []
    while date <= end_date:
        for j in ["00", "08", "16"]:
            a = str(date) + " " + j + ":00:00"
            if a <= end:
                funding_time.append(pd.Timestamp(a))
        date = date + datetime.timedelta(days = 1)
    return funding_time

def get_oss_funding_data(exchange, kind, start_date, end_date, funding = True, input_coins = []):
    a0 = datetime.datetime.now()
    kind = kind.upper()
    start_date = start_date + datetime.timedelta(days = -1)
    dates = get_dates(start_date, end_date)
    oss_path = "/mnt/efs/fs1/data_center"
    if funding:
        oss_path = oss_path + '/funding_rate/'
        if exchange in ["ftx", "bybit", "gate", "kucoin", "deribit", "bibox", "ascendex"]:
            path = exchange + '-swap'
        elif exchange in ["okex5", "okx","okex"]:
            path = "okex5-swap"
        elif exchange == "binance":
            if kind == "USD":
                path = "binancecoinm-coinswap"
            elif kind in ["USDT", "BUSD"]:
                path = "binanceusdm-swap"
            else:
                print("binance kind error")
                return 
        else:
            print("exchange error")
            return
    else:
        oss_path = oss_path + "spreads/"
    object_dir = oss_path + path
    datas = {}
    filenames = list(os.listdir(object_dir))
    files = []
    for file in filenames:
        if file.split("-")[-1] == kind:
            files.append(file)
    
    if len(input_coins) == 0:
        for file in files:
            data = pd.DataFrame()
            currency = file.split("-")[0]
            datas[currency] = data.copy()
            for date in dates:
                path_file = object_dir + "/" + file + "/" + date + ".csv"
                try:
                    df = pd.read_csv(path_file)
                    if "timestamp" not in df.columns and "time" in df.columns:
                        df["timestamp"] = df["time"]
                    datas[currency] = pd.concat([datas[currency], df])
                except:
                    pass
            if "timestamp" not in datas[currency].columns and "time" in datas[currency]:
                datas[currency]["timestamp"] = datas[currency]["time"]
            datas[currency] = datas[currency].drop_duplicates(subset = ["timestamp"])
            datas[currency].index = range(len(datas[currency]))
    else:
        for currency in input_coins:
            datas[currency] = pd.DataFrame()
        for file in files:
            data = pd.DataFrame()
            currency = file.split("-")[0]
            if currency in input_coins:
                datas[currency] = data.copy()
                for date in dates:
                    path_file = object_dir + "/" + file + "/" + date + ".csv"
                    try:
                        df = pd.read_csv(path_file) 
                        if "timestamp" not in df.columns and "time" in df.columns:
                            df["timestamp"] = df["time"]
                        datas[currency] = pd.concat([datas[currency], df])
                    except:
                        pass
                if "timestamp" not in datas[currency].columns and "time" in datas[currency].columns:
                    datas[currency]["timestamp"] = datas[currency]["time"]
                datas[currency] = datas[currency].drop_duplicates(subset = ["timestamp"])
                datas[currency].index = range(len(datas[currency]))
    b0 = datetime.datetime.now()
    #print(f"get oss funding data: {b0-a0}")
    return datas

def get_dates(start_date, end_date):
    dates = []
    date = start_date
    while date <= end_date:
        dates.append(str(date))
        date = date + datetime.timedelta(days = 1)
    return dates


def unify_funding_data(datas, exchange, funding_time):
    a0 = datetime.datetime.now()
    datas_unified = {}
    for key in datas.keys():
        data = datas[key]
        df = pd.DataFrame(columns = ["dt", "UTC-time", "funding", "symbol"])
        df_ftx = pd.DataFrame(columns = ["dt", "UTC-time", "funding", "symbol"])
        for i in data.index:
            if len(data.loc[i, "timestamp"]) <13:
                data.drop(i, axis = 0, inplace = True)
        data.index = range(len(data))
        try:
            df["UTC-time"] = data["timestamp"]
            if exchange == "kucoin":
                df["dt"]  = df["UTC-time"].apply(lambda x : datetime.datetime.strptime(x[:19].replace("T", " "), "%Y-%m-%d %H:%M:%S") + datetime.timedelta(hours = 12))
            else:
                df["dt"]  = df["UTC-time"].apply(lambda x : datetime.datetime.strptime(x[:19].replace("T", " "), "%Y-%m-%d %H:%M:%S") + datetime.timedelta(hours = 8))
            df = df.drop_duplicates(subset = ["dt", "funding"])
            if exchange in ['binance', 'okex5', "okx","okex",'ftx', 'deribit', "bibox", "ascendex", 'bybit']:
                df["funding"] = data["funding_rate"]
            elif exchange in ['kucoin']:
                df['funding'] = data['value']
            elif exchange == 'gate':
                df['funding'] = data['funding_rate']
                df['symbol'] = key
            else:
                print("exchange error: unify_funding_data")
                return 
            if exchange != 'gate':
                df["symbol"] = data["symbol"]
        except:
            # print(f"{exchange} {key} funding data NA")
            pass        
        
        if exchange == 'ftx':
            for time in funding_time:
                last_time = time + datetime.timedelta(hours = -7)
                a = df[((df['dt'] >= last_time) & (df['dt'] <= time))]
                if len(a) == 8:
                    df_ftx.loc[time, "funding"] = sum(a['funding'].values)
                    df_ftx.loc[time, "dt"] = time
                    df_ftx.loc[time, "UTC-time"] = a['UTC-time'].values[-1]
                    df_ftx.loc[time, "symbol"] = a['symbol'].values[-1]
        df_ftx.index = range(len(df_ftx))
        if exchange != "ftx":
            df = df.drop_duplicates(subset = ["dt", "funding"])
            datas_unified[key] = df[((df["dt"] >= funding_time[0]) & (df["dt"] <= funding_time[-1]))].copy()
        else:
            df_ftx = df_ftx.drop_duplicates(subset = ["dt", "funding"])
            datas_unified[key] = df_ftx[((df_ftx["dt"] >= funding_time[0]) & (df_ftx["dt"] <= funding_time[-1]))].copy()
    b0 = datetime.datetime.now()
    #print(f"unify funding data: {b0-a0}")
    return datas_unified

def get_funding_diff(data1_unified, data2_unified, funding_time, filled = False, log_out = False):
    coins1  = set(data1_unified.keys())
    coins2 = set(data2_unified.keys())
    coins = coins1 & coins2
    funding_diff = pd.DataFrame(index = list(coins), columns = funding_time)
    for coin in coins:
        df1 = data1_unified[coin]
        df2 = data2_unified[coin]
        for time in funding_time:
            if (time in df1["dt"].values) and (time in df2["dt"].values):
                df_1 = df1[df1["dt"] == time].copy()
                df_2 = df2[df2["dt"] == time].copy()
                funding_diff.loc[coin, time] = df_1["funding"].values[-1] - df_2["funding"].values[-1]
            else:
                funding_diff.loc[coin, time] = np.nan
    funding_diff = funding_diff.sort_index(axis = 1 , ascending = False)
    na_filled = pd.DataFrame(columns = ["nan"])
    if filled:
        for coin in coins:
            a = list(funding_diff.loc[coin].values)
            na = np.where(np.isnan(a))[0]
            na_filled.loc[coin, "nan"] = list(funding_diff.columns[na])
            for i in na:
                col = funding_diff.columns[i]
                if i+1 <= len(funding_diff.columns) -1 and funding_diff.columns[i+1] != np.nan:
                    col_l = funding_diff.columns[i+1]
                    funding_diff.loc[coin, col] = funding_diff.loc[coin, col_l]
    if log_out:
        for coin in coins:
            a = list(funding_diff.loc[coin].values)
            if sum(np.isnan(a)) > 0:
                na = list(funding_diff.loc[coin][np.isnan(a)].index.values)
                na.sort()
                if len(na) > 1:
                    for i in range(1, len(na)):
                        if na[i] - na[i - 1] > np.timedelta64(8, "h"):
                            print(f"{coin} funding NA: {na[i]} to {na[i - 1]}")
                            break
                elif na[0] != np.datetime64(funding_time[0]) or na[-1] != np.datetime64(funding_time[-1]):
                    print(f"{coin} funding NA: {na[0]} to {na[-1]}")
    return funding_diff, na_filled

def get_funding_spot(data_unified, funding_time, filled = True, log_out = False):
    a0 = datetime.datetime.now()
    coins = list(data_unified.keys())
    funding_diff = pd.DataFrame(index = coins, columns = funding_time)
    for coin in coins:
        df = data_unified[coin]
        for time in funding_time:
            if time in df["dt"].values:
                df_1 = df[df["dt"] == time].copy()
                funding_diff.loc[coin, time] = df_1["funding"].values[-1]
            else:
                funding_diff.loc[coin, time] = np.nan
    funding_diff = funding_diff.sort_index(axis = 1 , ascending = False)
    na_filled = pd.DataFrame(columns = ["nan"])
    if filled:
        for coin in coins:
            a = list(funding_diff.loc[coin].values)
            na = np.where(np.isnan(a))[0]
            na_filled.loc[coin, "nan"] = list(funding_diff.columns[na])
            for i in na:
                col = funding_diff.columns[i]
                if i+1 <= len(funding_diff.columns) -1 and funding_diff.columns[i+1] != np.nan:
                    col_l = funding_diff.columns[i+1]
                    funding_diff.loc[coin, col] = funding_diff.loc[coin, col_l]
    if log_out:
        for coin in coins:
            a = list(funding_diff.loc[coin].values)
            if sum(np.isnan(a)) > 0:
                na = list(funding_diff.loc[coin][np.isnan(a)].index.values)
                na.sort()
                if len(na) > 1:
                    for i in range(1, len(na)):
                        if na[i] - na[i-1] > np.timedelta64(8, "h"):
                            print(f"{coin} funding NA: {na[i]} to {na[i-1]}")
                            break
                elif na[0] == np.datetime64(funding_time[0]) or na[-1] == np.datetime64(funding_time[-1]):
                    print(f"{coin} funding NA: {na[0]} to {na[-1]}")
    #             if jud:
    #                 print(f"{coin} funding data NA at {funding_diff.loc[coin][np.isnan(a)].index.values}")
    b0 = datetime.datetime.now()
    #print(f"funding spot: {b0-a0}")
    return funding_diff, na_filled
def get_klines(exchange, coin, contract, start:datetime.datetime, end:datetime.datetime, dt = True, contractsize = pd.DataFrame(), log = True):
    # start and end are UTC+8
    #contract should in ["usdt-swap", "usd-swap", "spot", "busd-swap"]
    if contract not in ["usdt-swap", "usd-swap", "spot", "busd-swap", "usdc-swap", "spot-usd"]:
        print("get_klines: contract error")
        return data
    data = pd.DataFrame(columns = ["time", "open", "high", "low", "close", "volume", "exchange", "symbol"])
    path = "/mnt/efs/fs1/data_center/klines/"
    exchange = exchange.lower()
    contract = contract.lower()
    coin = coin.upper()
    dates = get_dates((start + datetime.timedelta(days = -1)).date(), end.date())
    if exchange in ["ok", "okx", "okex5", "okexv5", "o", "okex"]:
        exchange = "okex"
    elif exchange in ["b", "bin", "binance"]:
        exchange = "binance"
    elif exchange in ["gateio", "g", "gate"]:
        exchange = "gate"
    elif exchange in ["bybit", "by"]:
        exchange = "bybit"
    elif exchange in ["ftx", "f"]:
        exchange = "ftx"
    elif exchange in ["kucoin", "k"]:
        exchange = "kucoin"
    elif exchange in ["bibox", "bi"]:
        exchange = "bibox"
    elif exchange in ["ascendex", "asc"]:
        exchange = "ascendex"
    elif exchange in ["coinbase", "coin"]:
        exchange = "coinbase"
    else:
        print("get_klines: exchange error")
        return data
    if exchange == "binance":
        if contract in ["usdt-swap", "busd-swap"]:
            path = path + "binance-swap/" 
        elif contract == "usd-swap":
            path = path + "binancecoin-swap/"
        elif contract == "spot":
            path = path + "binance-spot/"
        else:
            print("contract error")
            return data
    elif exchange == "okex":
        if "spot" in contract:
            path = path + "okex5-spot/"
        else:
            path = path + "okex5-swap/"
    elif exchange == "gate":
        if "spot" in contract:
            path = path + "gateio-spot/"
        else:
            path = path + "gate-swap/"
    elif exchange == "kucoin":
        if "spot" in contract:
            path = path + "kucoin-spot/"
        else:
            path = path + "kucoinfutures-swap/"
    elif exchange in ["bybit", "ftx", "bibox", "ascendex", "coinbase"]:
        if "spot" in contract:
            path = path + f"{exchange}-spot/"
        else:
            path = path + f"{exchange}-swap/"
    else:
        print("exchange error")
        return data
    if contract == "spot":
        path = path + coin + "-USDT/"
    elif contract == "spot-usd":
        path = path + coin + "-USD/"
    else:
        path = path + coin + "-" + contract.split("-")[0].upper() + "/"
    for date in dates:
        try:
            df = pd.read_csv(path + date + ".csv")
            data = pd.concat([data, df])
        except:
            if log:
                print(f"{exchange} {coin} {contract} {date} klines data NA")
            pass
    if dt:
        data["dt"] = data["time"].apply(lambda x: datetime.datetime.strptime(x[:19],'%Y-%m-%d %H:%M:%S') + datetime.timedelta(hours = 8))
    if contractsize.shape[0] != 0 and "spot" not in contract:
        if coin in contractsize.index and (exchange + "-usd-swap" in contractsize.columns or exchange + "-usdt-swap" in contractsize.columns):
            if exchange in ["ftx", "bybit"] and contract == "usdt-swap":
                coin_size = 1
            else:
                coin_size = contractsize.loc[coin, exchange + "-" + contract]
        else:
            print(f"{exchange} {coin} {contract} contractsize NA")
            coin_size = np.nan
        if contract == "usd-swap":
            data["volume_U"] = data["volume"] * coin_size
            if coin != "BTC" and exchange == "gate":
                key = f"gate/btc-usdt"
                key = bytes(key, encoding = "utf8")
                r = readData.read_redis()
                price_data = r.hgetall(key)
                if b'bid0_price' in price_data.keys():
                    price_btc = eval(price_data[b'bid0_price'])
                else:
                    price_btc = np.nan
                data["volume_U"] = data["volume_U"] * data["close"] * price_btc
        else:
            data["volume_U"] = data["volume"] * coin_size * data["close"]
    elif "spot" in contract:
        if exchange in ["gate", "ftx"]:
            data["volume_U"] = data["volume"]
        else:
            data["volume_U"] = data["volume"] * data["close"]
    data = data[data["dt"] >= start]
    data = data[data["dt"] <= end]
    data = data.drop_duplicates()
    data.index = range(len(data))
    return data
def get_funding_sum(funding_diff):
    a0 = datetime.datetime.now()
    coins = list(funding_diff.index)
    funding_sum = pd.DataFrame(columns = ['last_dt', "1t", "1d", '3d', '7d', '15d', '30d'])
    for coin in coins:
        for col in funding_sum.columns[1:]:
            num = eval(col[:-1]) * 3
            funding_sum.loc[coin, col] = sum(funding_diff.loc[coin][:num].values)
        funding_sum.loc[coin, "1t"] = sum(funding_diff.loc[coin][:1].values)
    funding_sum['last_dt'] = funding_diff.columns[0]
    b0 = datetime.datetime.now()
    #print(f"funding sum: {b0-a0}")
    return funding_sum.sort_values(by = "15d", ascending = False)

def get_vol():
    api = MarketAPI()
    vol = {}
    for instType in ["SPOT", "SWAP", "FUTURES", "OPTION"]:
        response = api.get_tickers(instType)
        ret = response.json() if response.status_code == 200 else {"data": []}
        data = {i["instId"]: i for i in ret["data"]}
        vol.update(data)
    return vol

def run_funding(exchange1, kind1, exchange2, kind2, start_date, end_date, 
                filled = False, play = False, log_out = False, input_coins = []):
    path = os.environ['HOME'] + "/parameters/config_buffet/dt"
    contractsize = pd.read_csv(f"{path}/contractsize.csv", index_col = 0)
    start_time = datetime.datetime.now()
    kind1 = kind1.lower().replace("margin", "spot")
    kind2 = kind2.lower().replace("margin", "spot")
    combo = exchange1 + "-" + kind1 + "-" + exchange2 +"-"+ kind2
    if log_out:
        print(f"{combo}:")
    funding_time = get_funding_time(start_date, end_date)
    if kind1 != "spot" and kind2 != "spot":
        data1= get_oss_funding_data(exchange1, kind1, start_date, end_date, input_coins = input_coins)
        data1_unified = unify_funding_data(data1, exchange1, funding_time)
        data2= get_oss_funding_data(exchange2, kind2, start_date, end_date, input_coins = input_coins)
        data2_unified = unify_funding_data(data2, exchange2, funding_time)
        funding_diff, na_filled = get_funding_diff(data1_unified, data2_unified,funding_time, filled, log_out)
    elif kind1 != "spot" and kind2 == "spot":
        data1= get_oss_funding_data(exchange1, kind1, start_date, end_date, input_coins = input_coins)
        data1_unified = unify_funding_data(data1, exchange1, funding_time)
        funding_diff, na_filled = get_funding_spot(data1_unified, funding_time, filled, log_out)
    elif kind1 == "spot" and kind2 != "spot":
        data2= get_oss_funding_data(exchange2, kind2, start_date, end_date, input_coins = input_coins)
        data2_unified = unify_funding_data(data2, exchange2, funding_time)
        funding_diff, na_filled = get_funding_spot(data2_unified, funding_time, filled, log_out)
    else:
        print("kind error")
    
    funding_sum = get_funding_sum(funding_diff)
    ret = get_vol()
    for coin in funding_sum.index:
        vol = {kind1: np.nan, kind2: np.nan}
        for kind in [kind1, kind2]:
            k = f"{coin.upper()}-{kind.upper()}-SWAP" if kind != "spot" else coin.upper() + "-USDT"
            info = ret[k] if k in ret.keys() else {"volCcy24h": np.nan, "last": np.nan}
            vol[kind] = float(info["volCcy24h"]) * float(info["last"]) if kind != "spot" else float(info["volCcy24h"])
        funding_sum.loc[coin, "volume_U_24h"] = min(vol.values())
    if play:
        transfer = {}
        transfer["usdc"] = "usdc_swap"
        transfer["usdt"] = "u_swap"
        transfer["usd"] = "c_swap"
        transfer["spot"] = "spot"
        transfer["busd"] = "busd_swap"
        if exchange1 == exchange2:
            if exchange1 != "okex5":
                title_name = exchange1 + "_"
            else:
                title_name = "okx_"
        else:
            if exchange1 != "okex5" and exchange2 != "okex5":
                title_name = exchange1 + "_" + exchange2 + "_"
            elif exchange1 == "okex5":
                title_name = "okx_" + exchange2 + "_"
            else:
                title_name = exchange1 + "_" + "okx_"
        title_name = title_name + transfer[kind1] + "_" + transfer[kind2]
        start_time = datetime.datetime.now()
        result = funding_diff.T.sort_index()
        result = result.sort_index(axis = 1)
        result = result.cumsum()
        tabs = []
        for coin in result.columns:
            df = pd.DataFrame()
            df[coin] = result[coin]
            #df = df.fillna("")
            p = draw_ssh.line(df, play = False, title = title_name)
            tab = Panel(child = p, title = coin)
            tabs.append(tab)
        t = Tabs(tabs = tabs)
        show(t)
        end_time = datetime.datetime.now()
    return funding_sum , funding_diff, na_filled

def get_last_influx_funding(exchange_name, pair_name):
    #funding_time: UTC time
    dataname = "funding"
    if exchange_name in ["okex", "okexv5", "okex5", "okx", "ok", "o"]:
        exchange_name = "okexv5"
    elif exchange_name in ["binance", "b"]:
        exchange_name = "binance"
    elif exchange_name in ["gate", "gateio", "g"]:
        exchange_name = "gate"
    else:
        pass
    data = pd.DataFrame(columns = ["next_fee", "rate"])
    if pair_name.split("-")[1] == "margin":
        data.loc[0] = [0, 0]
        return data
    num = 0
    while len(data) == 0:
        a = f'''
            select next_fee, rate from {dataname} where exchange = '{exchange_name}' and pair = '{pair_name}' and time > now() - 10m order by time desc LIMIT 1
            '''
        data = readData.read_influx(a, db = "market_data", en = "INFLUX_MARKET_URI")
        num += 1
        if num > 3:
            data = pd.DataFrame(columns = ["next_fee", "rate"])
            data.loc[0] = [np.nan, np.nan]
    if len(data) == 0:
        data = pd.DataFrame(columns = ["next_fee", "rate"])
        data.loc[0] = [np.nan, np.nan]
    return data

def get_influx_funding_data(timestamp:datetime.datetime, exchange_name, pair_name):
    #funding_time: UTC+8 time
    dataname = "funding"
    df = pd.DataFrame(columns = ["UTC-time","dt", "current", "next"], index = [0])
    if exchange_name in ["okex", "okexv5", "okex5", "okx", "ok", "o"]:
        exchange_name = "okexv5"
    elif exchange_name in ["binance", "b"]:
        exchange_name = "binance"
    elif exchange_name in ["gate", "gateio", "g"]:
        exchange_name = "gate"
    else:
        print("exhange_name error")
        return 
    if "swap" not in pair_name:
        df.loc[0, "current"] = 0
        df.loc[0, "next"] = 0
        return df
    
    end = readData.transfer_time(timestamp)
    start = readData.transfer_time(timestamp + datetime.timedelta(minutes = -10))
    a = f'''
        select next_fee, rate from {dataname} where time >= '{start}' and time <= '{end}' and exchange = '{exchange_name}' and pair = '{pair_name}' order by time desc LIMIT 1
        '''
    data = readData.read_influx(a, db = "market_data", en = "INFLUX_MARKET_URI")
    if len(data) > 0:
        name = 0
        df.loc[name, "UTC-time"] = data.loc[0, "time"]
        df.loc[name, "dt"] = data.loc[0, "dt"]
        df.loc[name, "current"] = data.loc[0, "rate"]
        df.loc[name, "next"] = data.loc[0, "next_fee"]
        if exchange_name == "binance":
            df.loc[name, "next"] = np.nan
    return df
def observe_dt_trend(start_date = datetime.date(2021,1,1),
                    end_date = datetime.date.today(),
                    logout = False):
    trade_combo = pd.DataFrame(columns = ["exchange1", "kind1", "exchange2", "kind2", "name"])
    trade_combo.loc[0] = ["okex", "usdt", "okex", "usd", "_dt"]
    # end_date = datetime.date.today()
    # start_date = datetime.date(2021,1,1)# 北京时间
    input_coins = ["BTC", "ETH"]
    
    result = pd.DataFrame(columns = ["next", "current", "1d_avg", "3d_avg", "7d_avg", "15d_avg", "30d_avg", "7d_sum", "15d_sum", "30d_sum"])
    for i in trade_combo.index:
        exchange1 = trade_combo.loc[i, "exchange1"]
        exchange2 = trade_combo.loc[i, "exchange2"]
        kind1 = trade_combo.loc[i, "kind1"]
        kind2 = trade_combo.loc[i, "kind2"]
        if trade_combo.loc[i, "name"] == "_dt":
            is_play = True
        else:
            is_play = False
        funding_sum, funding_diff, na_filled = run_funding(exchange1, kind1, exchange2, kind2, start_date, end_date, log_out = logout, input_coins = input_coins, play = is_play)
        for coin in input_coins:
            data = pd.DataFrame(columns = ["1d_avg", "3d_avg", "7d_avg", "15d_avg", "30d_avg", "7d_sum", "15d_sum", "30d_sum","vol_24h"])
            location = coin + trade_combo.loc[i, "name"]
            for col in ["1d_avg", "3d_avg", "7d_avg", "15d_avg", "30d_avg"]:
                f_col = col.split("_")[0]
                num = eval(f_col.split("d")[0]) * 3
                data.loc[location, col] = funding_sum.loc[coin, f_col] / num
            for col in ["7d_sum", "15d_sum", "30d_sum"]:
                f_col = col.split("_")[0]
                data.loc[location, col] = funding_sum.loc[coin, f_col]
            data.loc[location, "vol_24h"] = format(int(funding_sum.loc[coin, "volume_U_24h"]), ",") if not np.isnan(funding_sum.loc[coin, "volume_U_24h"]) else np.nan
            df = pd.DataFrame(columns = ["current", "next"])
            if kind1 != "spot" and kind2 != "spot":
                pair_name = coin.lower() +"-"+ kind1 + "-swap"
                df1 = get_last_influx_funding(exchange_name = exchange1, pair_name = pair_name)
                pair_name = coin.lower() +"-"+ kind2 + "-swap"
                df2 = get_last_influx_funding(exchange_name = exchange2, pair_name = pair_name)
                df.loc[coin, "current"] = df1["rate"].values[-1] - df2["rate"].values[-1] 
                df.loc[coin, "next"] = df1["next_fee"].values[-1] - df2["next_fee"].values[-1] 
            elif kind1 == "spot" and kind2 != "spot":
                pair_name = coin.lower() +"-"+ kind2 + "-swap"
                df2 = get_last_influx_funding(exchange_name = exchange2, pair_name = pair_name)
                df.loc[coin, "current"] = df2["rate"].values[-1] 
                df.loc[coin, "next"] = df2["next_fee"].values[-1] 
            elif kind1 != "spot" and kind2 == "spot":
                pair_name = coin.lower() +"-"+ kind1 + "-swap"
                df1 = get_last_influx_funding(exchange_name = exchange1, pair_name = pair_name)
                df.loc[coin, "current"] = df1["rate"].values[-1] 
                df.loc[coin, "next"] = df1["next_fee"].values[-1]
            data.loc[location, "current"] = df.loc[coin, "current"]
            data.loc[location, "next"] = df.loc[coin, "next"]
            result = pd.concat([result, data])
    exchange1="okex5"
    kind1 = 'usdt'
    exchange2='okex5'
    kind2 = 'usd'
    end_date = datetime.date.today()
    start_date = end_date + datetime.timedelta(days = -33)
    funding_sum, funding_diff, na_filled = run_funding(exchange1, kind1, exchange2, kind2, start_date, end_date, log_out = False, play = False)
    data = pd.DataFrame(columns = ["1d_avg", "3d_avg", "7d_avg", "15d_avg", "30d_avg", "7d_sum", "15d_sum", "30d_sum","vol_24h"])
    for coin in funding_sum.index:
        location = coin
        for col in ["1d_avg", "3d_avg", "7d_avg", "15d_avg", "30d_avg"]:
            f_col = col.split("_")[0]
            num = eval(f_col.split("d")[0]) * 3
            data.loc[location, col] = funding_sum.loc[coin, f_col] / num
        for col in ["7d_sum", "15d_sum", "30d_sum"]:
            f_col = col.split("_")[0]
            data.loc[location, col] = funding_sum.loc[coin, f_col]
        if np.isnan(funding_sum.loc[coin, "volume_U_24h"]):
            data.loc[location, "vol_24h"] = np.nan
        else:
            data.loc[location, "vol_24h"] = format(int(funding_sum.loc[coin, "volume_U_24h"]), ",")
        df = pd.DataFrame(columns = ["current", "next"])
        if kind1 != "spot" and kind2 != "spot":
            pair_name = coin.lower() +"-"+ kind1 + "-swap"
            df1 = get_last_influx_funding(exchange_name = exchange1, pair_name = pair_name)
            pair_name = coin.lower() +"-"+ kind2 + "-swap"
            df2 = get_last_influx_funding(exchange_name = exchange2, pair_name = pair_name)
            df.loc[coin, "current"] = df1["rate"].values[-1] - df2["rate"].values[-1] 
            df.loc[coin, "next"] = df1["next_fee"].values[-1] - df2["next_fee"].values[-1] 
        elif kind1 == "spot" and kind2 != "spot":
            pair_name = coin.lower() +"-"+ kind2 + "-swap"
            df2 = get_last_influx_funding(exchange_name = exchange2, pair_name = pair_name)
            df.loc[coin, "current"] = df2["rate"].values[-1] 
            df.loc[coin, "next"] = df2["next_fee"].values[-1] 
        elif kind1 != "spot" and kind2 == "spot":
            pair_name = coin.lower() +"-"+ kind1 + "-swap"
            df1 = get_last_influx_funding(exchange_name = exchange1, pair_name = pair_name)
            df.loc[coin, "current"] = df1["rate"].values[-1] 
            df.loc[coin, "next"] = df1["next_fee"].values[-1]
        data.loc[location, "current"] = df.loc[coin, "current"]
        data.loc[location, "next"] = df.loc[coin, "next"]
    result = pd.concat([result, data])
    for col in result.columns:
        if col != "vol_24h":
            result[col] = result[col].apply(lambda x: format(x, ".3%"))
    funding = funding_diff.T
    cols = list(funding.columns)
    cols.remove("BTC")
    cols.remove("ETH")
    cols = ["BTC", "ETH"] + cols
    funding = funding[cols]
    return result, funding
        
def daily_observe_trend(start_date = datetime.date(2021,1,1),
                        end_date = datetime.date.today(),
                        logout = False):
    trade_combo = pd.DataFrame(columns = ["exchange1", "kind1", "exchange2", "kind2", "name"])
    trade_combo.loc[0] = ["binance", "usdt", "okex5", "usd", "_dt"]
    trade_combo.loc[1] = ["okex5", "usdt", "binance", "usd", "_dt"]
    trade_combo.loc[2] = ["okex5", "usdt", "okex5", "spot", "_ssfo"]
    trade_combo.loc[3] = ["bybit", "usdt", "okex5", "usd", "_dt"]
    trade_combo.loc[4] = ["ftx", "usdt", "okex5", "usd", "_dt"]
    end_date = datetime.date.today()
    start_date = datetime.date(2021,1,1)# 北京时间
    input_coins = ["BTC", "ETH"]
    funding_diff,funding_usdt,close=get_data(exchange='okex')
    plot_market_u(funding_diff,funding_usdt,close,start='2021',end='2022',title='DT')
    
    for i in trade_combo.index:
        exchange1 = trade_combo.loc[i, "exchange1"]
        exchange2 = trade_combo.loc[i, "exchange2"]
        kind1 = trade_combo.loc[i, "kind1"]
        kind2 = trade_combo.loc[i, "kind2"]
        funding_sum, funding_diff, na_filled = run_funding(exchange1, kind1, exchange2, kind2, start_date, end_date, log_out = False, input_coins = input_coins, play = True)

    # result = pd.DataFrame(columns = ["next", "current", "1d_avg", "3d_avg", "7d_avg", "15d_avg", "30d_avg", "7d_sum", "15d_sum", "30d_sum","short_trend", "short_reverse", "long_trend", "long_reverse"])
    # funding_all = pd.DataFrame()
    # rate = {}
    # rate["1d"] = 0.35
    # rate["3d"] = 0.3
    # rate["7d"] = 0.2
    # rate["15d"] = 0.1
    # rate["30d"] = 0.05
    # funding_all = pd.DataFrame()
    
        # a = funding_diff.T.sort_index()
        # a = a.sort_index(axis = 1)
        # a = a.cumsum()
        # for col in a.columns:
        #     a.rename(columns = {col:col + trade_combo.loc[i, "name"]}, inplace = True)
        # funding_all = funding_all.join(a, how = "outer")
#         data = pd.DataFrame(columns = ["1d_avg", "3d_avg", "7d_avg", "15d_avg", "30d_avg", "7d_sum", "15d_sum", "30d_sum","vol_24h","short_trend", "short_reverse", "long_trend", "long_reverse"])
#         for coin in input_coins:
#             location = coin + trade_combo.loc[i, "name"]
#             for col in ["1d_avg", "3d_avg", "7d_avg", "15d_avg", "30d_avg"]:
#                 f_col = col.split("_")[0]
#                 num = eval(f_col.split("d")[0]) * 3
#                 data.loc[location, col] = funding_sum.loc[coin, f_col] / num
#             for col in ["7d_sum", "15d_sum", "30d_sum"]:
#                 f_col = col.split("_")[0]
#                 data.loc[location, col] = funding_sum.loc[coin, f_col]
#             data.loc[location, "vol_24h"] = format(int(funding_sum.loc[coin, "volume_U_24h"]), ",")
#             short_trend = 0
#             long_trend = 0
#             if data.loc[location, "1d_avg"] > data.loc[location, "3d_avg"]:
#                 short_trend += 0.4
#             else:
#                 long_trend += 0.4
#             if data.loc[location, "3d_avg"] > data.loc[location, "7d_avg"]:
#                 short_trend += 0.3
#             else:
#                 long_trend += 0.3
#             if data.loc[location, "7d_avg"] > data.loc[location, "15d_avg"]:
#                 short_trend += 0.2
#             else:
#                 long_trend += 0.2
#             if data.loc[location, "15d_avg"] > data.loc[location, "30d_avg"]:
#                 short_trend += 0.1
#             else:
#                 long_trend += 0.1
#             data.loc[location, "short_trend"] = short_trend
#             data.loc[location, "long_trend"] = long_trend

#             short_reverse = 0
#             long_reverse = 0
#             for day in ["1d", "3d", "7d", "15d", "30d"]:
#                 number = eval(day.split("d")[0]) * 3
#                 pos = sum(funding_diff.loc[coin].values[:number] > 0)
#                 neg = sum(funding_diff.loc[coin].values[:number] < 0)
#                 short_reverse += rate[day] * pos / number
#                 long_reverse += rate[day] * neg / number
#             data.loc[location, "short_reverse"] = short_reverse
#             data.loc[location, "long_reverse"] = long_reverse
#             df = pd.DataFrame(columns = ["current", "next"])
#             if kind1 != "spot" and kind2 != "spot":
#                 pair_name = coin.lower() +"-"+ kind1 + "-swap"
#                 df1 = get_last_influx_funding(exchange_name = exchange1, pair_name = pair_name)
#                 pair_name = coin.lower() +"-"+ kind2 + "-swap"
#                 df2 = get_last_influx_funding(exchange_name = exchange2, pair_name = pair_name)
#                 df.loc[coin, "current"] = df1["rate"].values[-1] - df2["rate"].values[-1] 
#                 df.loc[coin, "next"] = df1["next_fee"].values[-1] - df2["next_fee"].values[-1] 
#             elif kind1 == "spot" and kind2 != "spot":
#                 pair_name = coin.lower() +"-"+ kind2 + "-swap"
#                 df2 = get_last_influx_funding(exchange_name = exchange2, pair_name = pair_name)
#                 df.loc[coin, "current"] = df2["rate"].values[-1] 
#                 df.loc[coin, "next"] = df2["next_fee"].values[-1] 
#             elif kind1 != "spot" and kind2 == "spot":
#                 pair_name = coin.lower() +"-"+ kind1 + "-swap"
#                 df1 = get_last_influx_funding(exchange_name = exchange1, pair_name = pair_name)
#                 df.loc[coin, "current"] = df1["rate"].values[-1] 
#                 df.loc[coin, "next"] = df1["next_fee"].values[-1]
#             data.loc[location, "current"] = df.loc[coin, "current"]
#             data.loc[location, "next"] = df.loc[coin, "next"]
#         result = pd.concat([result, data])
#     for coin in input_coins:
#         klines = get_klines(exchange = "okex", contract = "spot", coin = coin,
#                           start = datetime.datetime.combine(start_date, datetime.datetime.min.time()),
#                           end = datetime.datetime.now(), log = False)
#         klines.index = klines["dt"]
#         funding_all = funding_all.join(klines["close"], how = "left")
#         funding_all.rename(columns = {"close": coin + "_close"}, inplace = True)
#     funding_all.fillna(method = "ffill", inplace = True)
#     tabs = []
#     for coin in input_coins:
#         cols = []
#         for x in ["_dt", "_ssfo", "_close"]:
#             cols.append(coin.upper() + x)
#         data = funding_all.loc[:, cols].copy()
#         p = draw_ssh.line_doubleY(data, right_columns = [coin.upper() + "_close"],play = False, title = coin.upper())
#         tab = Panel(child = p, title = coin)
#         tabs.append(tab)
#     t = Tabs(tabs = tabs)
#     show(t)
   
#     for col in result.columns:
#         if col != "vol_24h":
#             result[col] = result[col].apply(lambda x: format(x, ".3%"))
#     return result

def daily_get_funding(logout = False, save = True, file_path = ""):
    trade_combo = pd.DataFrame(columns = ["exchange1", "kind1", "exchange2", "kind2"])
    trade_combo.loc[0] = ["gate", "spot", "gate", "usdt"]
    trade_combo.loc[1] = ["okex5", "spot", "okex5", "usdt"]
    trade_combo.loc[2] = ["binance", "spot", "binance", "usdt"]
    trade_combo.loc[3] = ["binance", "usdt", "binance", "usd"]
    trade_combo.loc[4] = ["binance", "usdt", "binance", "busd"]
    trade_combo.loc[5] = ["okex5", "usdt", "okex5", 'usd']
    trade_combo.loc[6] = ["gate", 'usdt', "gate", 'usd']
    trade_combo.loc[7] = ["gate", "usdt", "binance", "usdt"]
    trade_combo.loc[8] = ["binance", "usdt", "okex5", "usdt"]
    trade_combo.loc[9] = ["gate", "usdt", "okex5", "usdt"]
    trade_combo.loc[10] = ["binance", "usdt", "bybit", "usdt"]
    trade_combo.loc[11] = ["binance", "usdt", "ftx", "usdt"]
    trade_combo.loc[12] = ["binance", "usd", "okex5", "usd"]
    trade_combo.loc[13] = ["bybit", "usdt", "okex5", "usdt"]
    trade_combo.loc[14] = ["kucoin", "usdt", "kucoin", "spot"]
    trade_combo.loc[15] = ["kucoin", "usdt", "okex5", "usdt"]
    trade_combo.loc[16] = ["kucoin", "usdt", "binance", "usdt"]
    trade_combo.loc[17] = ["bybit", "spot", "bybit", "usdt"]
    trade_combo.loc[18] = ["okex5", "spot", "okex5", "usd"]
    trade_combo.index = range(len(trade_combo))
    transfer = {}
    transfer["usdt"] = "u_swap"
    transfer["usd"] = "c_swap"
    transfer["spot"] = "spot"
    transfer["busd"] = "busd_swap"
    end_date = datetime.date.today() + datetime.timedelta(days = -1)
    start_date = end_date + datetime.timedelta(days = -31)# 北京时间
    all_datas = {}
    num = 1
    for i in trade_combo.index:
        print("combo: %d / %d " % (num, len(trade_combo)))
        exchange1 = trade_combo.loc[i, "exchange1"]
        exchange2 = trade_combo.loc[i, "exchange2"]
        kind1 = trade_combo.loc[i, "kind1"]
        kind2 = trade_combo.loc[i, "kind2"]
        
        if exchange1 == exchange2:
            if exchange1 != "okex5":
                combo = exchange1 + "_"
            else:
                combo = "okx_"
        else:
            if exchange1 != "okex5" and exchange2 != "okex5":
                combo = exchange1 + "_" + exchange2 + "_"
            elif exchange1 == "okex5":
                combo = "okx_" + exchange2 + "_"
            else:
                combo = exchange1 + "_" + "okx_"
        combo = combo + transfer[kind1] + "_" + transfer[kind2]
        all_datas[combo] = run_funding(exchange1, kind1, exchange2, kind2, start_date, end_date, play = False, log_out = logout)
        num = num + 1
    if save:
        print("start save")
        sheet_names = list(all_datas.keys())
        path = os.path.dirname(os.getcwd())
        path = os.path.dirname(path)
        path_save = f"{path}/data/eva_result/"
        if not os.path.exists(path_save):
            os.makedirs(path_save)
        if file_path == "":
            writer = pd.ExcelWriter(f"{path}/data/eva_result/" + str(datetime.date.today()) + '.xlsx', engine='openpyxl')
        else:
            writer = pd.ExcelWriter(f"{file_path}/" + str(datetime.date.today()) + '.xlsx', engine='openpyxl')
        for sheet_name in sheet_names:
            data = all_datas[sheet_name][0].copy()
            if "volume_U_24h" in data.columns:
                data["volume_U_24h"] = data["volume_U_24h"].apply(lambda x: str(x) if np.isnan(x) else format(int(x), ","))
            for col in ["1t", "1d", "3d", "7d", "15d", "30d"]:
                data[col] = data[col].apply(lambda x: str(x) if np.isnan(x) else format(int(x), ".4%"))
            data.to_excel(excel_writer=writer, sheet_name=sheet_name, encoding="UTF-8")
            num += 1
        writer.save()
        writer.close()
        print("finished!")
    return all_datas

def get_data(exchange='binance'):
    
    base_coins = ['BTC','ETH']

    funding_diff=pd.read_csv(r'/home/tx/data/plot_data/'+exchange+'/funding_diff.csv',index_col=0)
    funding_diff.index=pd.to_datetime(funding_diff.index)

    funding_usdt=pd.read_csv(r'/home/tx/data/plot_data/'+exchange+'/funding_usdt.csv',index_col=0)
    funding_usdt.index=pd.to_datetime(funding_usdt.index)

    close=pd.read_csv(r'/home/tx/data/plot_data/'+exchange+'/kline.csv',index_col=0)
    close.index=pd.to_datetime(close.index)
    
    
    if exchange=='binance':

        dic={'funding_diff':["binance", "usdt", "binance", "busd"],
            'funding_usdt':["binance", "usdt", "binance", "spot"],
            'close':['binance']}
    else:
        dic={'funding_diff':["okex", "usdt", "okex", "usd"],
            'funding_usdt':["okex", "usdt", "okex", "spot"],
            'close':['okex']}

    # 获取funding
    start=funding_diff.index[-1]
    end_date = datetime.date.today()
    start_date=datetime.datetime.date(datetime.datetime.strptime(start.strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S'))

    for i in list(dic.keys())[0:-1]:
        df = run_funding(dic[i][0], dic[i][1], dic[i][2], dic[i][3], start_date, end_date, play = False, log_out = False,input_coins=base_coins)
        tmp_funding=(df[1].T)[base_coins]
        tmp_funding=tmp_funding.fillna(value=0)
        tmp_funding.sort_index(axis = 0,ascending = True,inplace = True)
        if i=='funding_diff':
            funding_diff=pd.concat([funding_diff,tmp_funding])
            funding_diff=funding_diff[~funding_diff.index.duplicated(keep = 'last')]
        else:
            funding_usdt=pd.concat([funding_usdt,tmp_funding])
            funding_usdt=funding_usdt[~funding_usdt.index.duplicated(keep = 'last')]
    # 获取kline
    start1=close.index[-1]
    tmp_close=pd.DataFrame()
    for i in base_coins:
        kline=get_klines(dic['close'][0], 
        i.lower(), 'usdt-swap', start=start1, end=datetime.datetime.now(),dt = True, contractsize = pd.DataFrame(), log = False)
        kline.set_index('time',inplace=True)
        kline.index=pd.to_datetime(kline.index)
        kline.set_index('dt',inplace=True)
        kline=kline.resample('8h',label='right').last()
        tmp=kline[['close']]
        tmp.columns=[i]
        tmp_close=pd.concat([tmp_close,tmp],axis=1)
    close=pd.concat([close,tmp_close])
    close=close[~close.index.duplicated(keep = 'last')]
    
    return funding_diff,funding_usdt,close


def plot_market_u(funding_diff,funding_usdt,close,start,end,title):
    tabs = []
    for coin in close.columns:
        out =pd.concat([funding_diff[[coin]],funding_usdt[[coin]],close[[coin]]],axis=1)
        out.columns=[coin+'_DT-O',coin+'_SSFO',coin+'_CLOSE']
        out=out[start:end]
        out[[coin+'_DT-O',coin+'_SSFO']]= out[[coin+'_DT-O',coin+'_SSFO']].cumsum()
        p = draw_ssh.line_doubleY(out,right_columns=[coin+'_CLOSE'],play = False, title =title)
        tab = Panel(child = p, title = coin)
        tabs.append(tab)
    t = Tabs(tabs = tabs)
    show(t)
