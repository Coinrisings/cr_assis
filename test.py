import requests, datetime, pytz
import pandas as pd
from IPython.display import display
from connectData import ConnectData
database = ConnectData()

ret = requests.get("https://www.okx.com/api/v5/market/history-index-candles?instId=BTC-USDT&after=1670493420000")
data = ret.json()['data']
spot_index = pd.DataFrame(columns = ["timestamp", "open", "high", "open", "close"])
for i in range(len(data)):
    info = data[i]
    ts, open, high, low, close, is_update = info
    if is_update == "1":
        timestamp = datetime.datetime.fromtimestamp(int(ts) / 1000, tz = datetime.timezone.utc)
        spot_index.loc[i] = [timestamp, float(open), float(high), float(low), float(close)]
    else:
        pass
spot_index["dt"] = spot_index["timestamp"].apply(lambda x: x.astimezone(pytz.timezone("Asia/ShangHai")))
display(spot_index)
# start = str(min(spot_index["timestamp"])).split("+")[0]
# end = str(max(spot_index["timestamp"])).split("+")[0]
# a = f"""
# SELECT last("bid0_price") as bid, last("ask0_price") as offer FROM orderbook_okex_swap WHERE time >= '{start}' and time <= '{end}' and symbol = 'btc-usdt-swap' GROUP BY time(1m) order by time asc LIMIT 10
# """
# database.load_influxdb(database = "market_data")
# ret = database.influx_clt.query(a)
# database.influx_clt.close()
# orderbook = pd.DataFrame(ret.get_points())
# orderbook["dt"] = orderbook["time"].apply(lambda x: datetime.datetime.strptime(x[:19], "%Y-%m-%dT%H:%M:%S") + datetime.timedelta(hours = 8))
# orderbook["dt"] = orderbook["dt"].apply(lambda x: x.tz_localize(pytz.timezone("Asia/ShangHai")))
# display(orderbook)
# result = pd.merge(spot_index, orderbook, on = "dt")
# result.sort_values(by = "dt", inplace= True)
# result["diff"] = (result["bid"] + result["offer"]) / 2 / result["close"] - 1
# result["funding"] = result["diff"].expanding().mean()
# display(result)
print(111111)