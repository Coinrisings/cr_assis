import datetime, requests, datetime, json, hashlib, hmac, base64
import pandas as pd
from cr_assis.pnl.dtfPnl import DtfPnl
from account.accountBase import AccountBase
# anta001 = AccountBase(deploy_id="anta_anta001@dt_okex_uswap_okex_cfuture_btc")
ht001 = AccountBase(deploy_id= "ht_ht001@ssf_okexv5_spot_okexv5_uswap_btc")
ht001.get_account_position()
account = ht001
account.start = datetime.datetime(2023,3,6,0,0,0)
account.end = datetime.datetime(2023,3,7,0,0,0)
third_pnl = account.get_third_pnl()
print(third_pnl)

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