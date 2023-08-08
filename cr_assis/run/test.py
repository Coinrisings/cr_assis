import datetime, time
import pandas as pd
from cr_assis.connect.connectOkex import ConnectOkex
from cr_assis.account.accountOkex import AccountOkex
from cr_assis.connect.connectOkex import ConnectOkex
from cr_assis.api.okex.accountApi import AccountAPI
from cr_assis.account.accountBinance import AccountBinance
from urllib.parse import urljoin, urlencode
import requests, json, time, hmac, hashlib
account = AccountOkex("test_hfok01@pt_okex_btc")
account.datacenter = "/Users/chelseyshao/Downloads"
# start = datetime.datetime(2023,8,1,0,0,0)
# end = datetime.datetime(2023,8,3,0,0,0)
# account.run_pnl(start, end, play = True)
account.start = datetime.datetime(2023,7,31)
account.end = datetime.datetime(2023,8,2)
orders = account.get_orders_data()
trade_data = account.handle_orders_data(play = True)
tpnl = account.get_tpnl()
account.get_equity()
(account.tpnl / account.adjEq).style.format({col: "{0:.4%}" for col in account.tpnl.columns})


apikey = "N4CcwMn3OsMvwmO19bSHsLNiv0FUQZw7KZoI04g4jk4ZK39RbYPDmfCKqwgiyEd4"
secret = "RsccANTQgmNnY73ZTXIyV3jhr3lvlkEZwOJgf8ab0YgUuZ03zzYXEnCBVhsAMNOm"
servertime = requests.get("https://api.binance.com/api/v1/time")
BASE_URL = "https://papi.binance.com"
headers = {
    'X-MBX-APIKEY': apikey
}
servertimeobject = json.loads(servertime.text)
servertimeint = servertimeobject['serverTime']
PATH = '/papi/v1/cm/allOrders'
timestamp = int(time.time() * 1000)
start_time = int(datetime.datetime.timestamp(datetime.datetime(2023,8,7,19,20,0)) * 1000)
end_time = int(datetime.datetime.timestamp(datetime.datetime(2023,8,7,21,0,0)) * 1000)
params = {
    "timestamp": timestamp,
    "startTime":start_time,
    "endTime":end_time,
    "symbol": "EOSUSD_PERP"
}
query_string = urlencode(params)
params['signature'] = hmac.new(secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()
url = urljoin(BASE_URL, PATH)
r = requests.get(url, headers=headers, params=params)

def get_okex_bills(name: str, start: datetime.datetime, end: datetime.datetime, adl = False) -> pd.DataFrame:
    api = AccountAPI()
    api.name = name
    api.load_account_api()
    start = int(datetime.datetime.timestamp(start)*1000)
    end = int(datetime.datetime.timestamp(end)*1000)
    ts = end
    data = []
    while ts >= start:
        if len(data) == 0:
            ret = api.get_bills_details(instType = "SWAP", limit=300, end=ts, type = 9).json()["data"] if adl else api.get_bills_details(instType = "SWAP", limit=300, end=ts).json()["data"]
        else:
            ret = api.get_bills_details(instType = "SWAP", limit=300, end=ts, type = 9, after=id).json()["data"] if adl else api.get_bills_details(instType = "SWAP", limit=300, end=ts, after=id).json()["data"]
        if len(ret) == 0 :
            break
        else:
            ts = int(ret[-1]["ts"])
            id = int(ret[-1]["billId"])
            data += ret
    df = pd.DataFrame(data)
    cols = ["bal", "balChg", "pnl", "price", "sz", "type"]
    df[cols] = df[cols].astype(float)
    df["dt"] = df["ts"].apply(lambda x: datetime.datetime.fromtimestamp(float(x) / 1000))
    return df

def get_okex_position(name: str) -> pd.DataFrame:
    api = AccountAPI()
    dataokex = ConnectOkex()
    api.name = name
    api.load_account_api()
    ret = api.get_positions().json()["data"]
    position = pd.DataFrame(columns = ["adl", "num", "mv", "raw"])
    for i in ret:
        pair = i["instId"]
        size = dataokex.get_contractsize(coin = pair.split("-")[0], contract=pair.replace(pair.split("-")[0], "")[1:].lower())
        position.loc[i["instId"]] = [float(i["adl"]), float(i["pos"]) * size, float(i["notionalUsd"]), str(i)]
    return position

def get_okex_balance(name: str):
    api = AccountAPI()
    api.name = name
    api.load_account_api()
    ret = api.get_account_balance().json()["data"][0]
    balance = pd.DataFrame(columns = ["num", "mv", "raw"])
    for i in ret["details"]:
        ccy = i["ccy"]
        balance.loc[ccy] = [float(i["eq"]), float(i["eqUsd"]), str(i)]
    return ret, balance

position = get_okex_position("bg_bg003")
equity, balance = get_okex_balance("bg_bg003")
df = get_okex_bills("bg_bg003", start = datetime.datetime(2023,7,13,0,0,0), end = datetime.datetime(2023,7,13,4,0,0,0), adl = False)
