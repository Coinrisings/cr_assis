import datetime, requests, datetime, json, hashlib, hmac, base64
import pandas as pd
import numpy as np
from cr_assis.pnl.dtfPnl import DtfPnl
from cr_assis.account.accountBase import AccountBase
import matplotlib.pyplot as plt

start = datetime.datetime(2023,3,17,0,0,0)
end = datetime.datetime(2023,3,24)

url = f"https://www.okx.com/api/v5/finance/savings/lending-rate-history?ccy=USDT&after={}&before={}"


print(datetime.date.today() - datetime.date(2022,9,28))
btc_number = 5
btc_price = 27708
adjEq = btc_number * btc_price
mul = 5
usdt = adjEq * mul
print(f"usd: {format(round(usdt / 100, 0), ',')}")
print(f"usdc: {format(round(btc_number * mul / 0.0001, 0), ',')}")
usd_mmr = 0.01
usdc_mmr = 0.01
mm = usdt * (usd_mmr + usdc_mmr)
mr = adjEq / mm
print(f"mr: {mr}")
data = pd.DataFrame(columns = ["upnl", "mr"])
price0 = 27699
for price in np.linspace(4000, price0, 20):
    upnl = (btc_price - price) * btc_number * mul
    mm1 = mm + upnl * 0.08
    mr1 = adjEq / mm1
    data.loc[price] = [upnl, mr1]

def get_new_coins_chance(combo):
    #加载数据
    master, slave = combo.split("-")
    exchange_master = master.split("_")[0]
    exchange_slave = slave.split("_")[0]
    kind1 = master.split("_")[1]
    kind2 = slave.split("_")[1] 
    if exchange_master in ["okex", "okx", "ok"]:
        exchange1 = "okex5"
    else:
        exchange1 = exchange_master
    if exchange_slave in ["okex", "okx", "ok"]:
        exchange2 = "okex5"
    else:
        exchange2 = exchange_slave
    end = datetime.datetime.now()+datetime.timedelta(hours=8)
    start = end - datetime.timedelta(days=365)
    f,fundings, na = run_funding(exchange1,kind1,exchange2,kind2,start.date(),end.date(),play =False,log_out = False)
    funding_ssf = -fundings.T.copy().sort_index()
    #新币
    data = pd.DataFrame()
    for coin in funding_ssf.columns:
        f = funding_ssf[coin].dropna()
        if len(f)>0 and (f.index[0] - funding_ssf.index[0]).days >= 30:
            data.loc[coin,'t'] = f.index[0]
    #新币30天数据
    res = pd.DataFrame()
    for coin in data.index:
        df = funding_ssf[[coin]].dropna().iloc[:30*3].reset_index()[[coin]]
        res = pd.concat([res,df],axis=1)
    #3天数据的可靠性
    data['f1'] = res.iloc[3*2:3*3].mean()
    data['f3'] = res.iloc[:3*3].mean()
    data['f_7'] = res.iloc[3*3:10*3].mean()
    data['f_15'] = res.iloc[3*3:3*18].mean()
    data['f_30'] = res.iloc[3*3:3*33].mean()
    dp = data[data['f3']<=-0.0001]
    plt.scatter(dp['f3'],dp['f_7'])
    plt.plot(dp['f3'],dp['f3'],c='r')
    dp = dp[['t','f1','f3','f_7','f_15','f_30']].sort_values('f_7')
    return dp

def get_future_date(timestamp: datetime.datetime) -> datetime.datetime:
    month = timestamp.month
    year = timestamp.year
    if month <= 3:
        future_date = datetime.date(year, 3, 31)
    elif month <= 6:
        future_date = datetime.date(year, 6, 30)
    elif month <= 9:
        future_date = datetime.date(year, 9, 30)
    else:
        future_date = datetime.date(year, 12, 31)
    return future_date

def get_future_contract(start: datetime.date, end: datetime.date) -> dict:
    timestamp = start
    future_timestamp = []
    future_contract = {}
    while timestamp <= end:
        future_date = get_future_date(timestamp)
        real_end = min(end, future_date)
        future_contract[future_date] = {"start": timestamp, "end": real_end}
        timestamp = future_date + datetime.timedelta(days = 1)
    return future_contract

def transfer_date_contract(date: datetime.date) -> str:
    year = str(date.year)[-2:]
    month = str(date.month) if len(str(date.month)) == 2 else "0" + str(date.month)
    day = str(date.day) if len(str(date.day)) == 2 else "0" + str(date.day)
    return year + month + day

def get_main_kline(contract: str, start_date: datetime.date, end_date: datetime.date) -> dict:
    future_contract = get_future_contract(start_date, end_date)
    headers = {
            "accept": "application/json",
            "content-type": "application/json"}
    kline = pd.DataFrame()
    min_time = datetime.datetime.min.time()
    for date, interval in future_contract.items():
        suffix = transfer_date_contract(date)
        start = int(datetime.datetime.timestamp(datetime.datetime.combine(interval["start"], min_time)) * 1000)
        end = int(datetime.datetime.timestamp(datetime.datetime.combine(interval["end"], min_time)) * 1000)
        timestamp = start
        while timestamp < end:
            url = f"https://www.okx.com/api/v5/market/history-candles?instId={contract}-230331&bar=4H&before={timestamp}&after={end}"
            response = requests.get(url, headers=headers)
            ret = response.json()
            data = pd.DataFrame(ret['data'])
            data.columns = ["ts", "open", "high", "lower", "close", "vol", "volCcy", "volCcyQuote", "confirm"]
            timestamp = int(data['ts'].values[-1])
            kline = pd.concat([kline, data])
    return kline
kline = get_main_kline(contract="BTC-USDT", start_date = datetime.date(2021,1,1), end_date = datetime.date.today())

start_date = datetime.date(2021,1,1)
end_date = datetime.date.today()
future_contract = get_future_contract(start_date, end_date)
for date in future_contract.keys():
    print(transfer_date_contract(date))

url = "https://www.okx.com/api/v5/market/history-candles?instId=BTC-USDT-230331&bar=4H"
headers = {
            "accept": "application/json",
            "content-type": "application/json"}
response = requests.get(url, headers=headers)
ret = response.json()
data = ret['data']

secret = "BC813A54ED28D0D35B9311D90CA8DBFE"
timestamp = datetime.datetime.now().astimezone(datetime.timezone.utc).isoformat(timespec='milliseconds').replace("+00:00", "Z")
sql = "/api/v5/account/max-loan?instId=BTC-USDT&mgnMode=cross"
message = timestamp + "GET" + sql
signature = base64.b64encode(hmac.new(bytes(secret, "utf-8"), bytes(message, "utf-8"), digestmod=hashlib.sha256).digest())
headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "OK-ACCESS-KEY": "3ec4b03a-92f1-4dd7-b3e1-0c30c0eb69cd",
            "OK-ACCESS-SIGN": signature,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": "8ha8GH57R@"
        }
url = f"https://www.okx.com{sql}"
response = requests.get(url, headers=headers)
print(response.json())


start = datetime.datetime(2022,11,3,0,0,0)
end = datetime.datetime(2022,11,5,0,0,0)
timestamp = start
result = {"prices": [], "market_caps": [], "total_volumes": []}
while timestamp <= end:
    unix_start = int(datetime.datetime.timestamp(timestamp))
    unix_end = int(datetime.datetime.timestamp(min(timestamp + datetime.timedelta(days = 1), end)))
    url = f"https://api.coingecko.com/api/v3/coins/ethereum/contract/0x15d4c048f83bd7e37d49ea4c83a07267ec4203da/market_chart/range?vs_currency=usd&from={unix_start}&to={unix_end}"
    response = requests.get(url)
    if response.status_code == 200:
        ret = response.json()
        for name, data in ret.items():
            result[name] = result[name] + ret[name]
    else:
        print(response.text)
    timestamp = timestamp + datetime.timedelta(days = 1)
data = pd.DataFrame(result['prices'])
data.columns = ["unix", "price"]
data['time'] = data['unix'].apply(lambda x: datetime.datetime.fromtimestamp(x/1000))
print(data)