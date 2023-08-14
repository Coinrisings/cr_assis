import datetime, time
import pandas as pd
from cr_assis.connect.connectOkex import ConnectOkex
from cr_assis.account.accountOkex import AccountOkex
from cr_assis.connect.connectOkex import ConnectOkex
from cr_assis.api.okex.accountApi import AccountAPI
from cr_assis.account.accountBinance import AccountBinance
from urllib.parse import urljoin, urlencode
import requests, json, time, hmac, hashlib
from cr_assis.pnl.binancePnl import get_all_trades

account = AccountBinance("test_hf01@pt_binance_usdt_portfolio")
all_trades = get_all_trades().sort_values("time").reset_index(drop=True)
all_trades["side"] = all_trades["side"].astype(str)
float_cols = ["price", "qty", "realizedPnl", "quoteQty", "commission", "time", "baseQty"]
all_trades[float_cols] = all_trades[float_cols].astype(float)
all_trades["coin"] = all_trades["coin"].str.upper()
all_trades["avg_price"] = all_trades["price"]
all_trades["turnover_side"] = all_trades.apply(lambda x: -1 if (x["side"].upper() == "BUY") or (x["isBuyer"] == True) else 1, axis = 1)
all_trades["is_usd"] = all_trades["pair"].apply(lambda x: True if x.split("-")[1].lower() == "usd" else False)
all_trades["real_number"] = all_trades.apply(lambda x: x["turnover_side"] * x["qty"] if not x["is_usd"] else (x["baseQty"] + x["turnover_side"] * x["realizedPnl"]) * x["turnover_side"], axis = 1)
all_trades["turnover"] = all_trades["real_number"] * all_trades["avg_price"]
all_trades["fee_U"] = all_trades.apply(lambda x: -x["commission"] if x["commissionAsset"].upper() in ["USDT", "USD", "BUSD", "USDC", "USDK", "BNB"] else -x["commission"] * x["avg_price"], axis =1)
account.trade_data = all_trades.copy()
account.get_tpnl()
account.get_equity()
print(account.tpnl / account.adjEq)

api = AccountAPI()
api.name = "wzok_001"
api.load_account_api()
response = api.get_max_loan(instId="SHIB-USDT", mgnMode="cross")
ret = response.json()
print(ret)

apikey = ""
secret = ""
servertime = requests.get("https://api.binance.com/api/v1/time")
BASE_URL = "https://papi.binance.com"
headers = {
    'X-MBX-APIKEY': apikey
}
servertimeobject = json.loads(servertime.text)
servertimeint = servertimeobject['serverTime']
PATH = '/papi/v1/cm/userTrades'
timestamp = int(time.time() * 1000)
start_time = int(datetime.datetime.timestamp(datetime.datetime(2023,8,10,0,0,0)) * 1000)
end_time = int(datetime.datetime.timestamp(datetime.datetime(2023,8,11,0,0,0)) * 1000)
params = {
    "timestamp": timestamp,
    "startTime":start_time,
    "endTime":end_time,
    "symbol": "BTCUSD_PERP",
    "limit": 1000
}
query_string = urlencode(params)
params['signature'] = hmac.new(secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()
url = urljoin(BASE_URL, PATH)
r = requests.get(url, headers=headers, params=params)