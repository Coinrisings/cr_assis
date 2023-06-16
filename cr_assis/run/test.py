import datetime, requests, datetime, json, os, time, hashlib, hmac
import pandas as pd
import numpy as np
from cr_assis.account.accountOkex import AccountOkex
from cr_assis.account.accountBinance import AccountBinance
from cr_assis.connect.connectOkex import ConnectOkex
from cr_assis.connect.updateOkexMarket import UpdateOkexMarket
from cr_assis.connect.updateGateWallet import UpdateGateWallet
from cr_assis.api.okex.marketApi import MarketAPI
from cr_assis.api.okex.publicApi import PublicAPI
from cr_assis.api.okex.accountApi import AccountAPI

from cr_monitor.mr.mrOkex import MrOkex
m = MrOkex()
m.price_range = [1]
m.run_account_mr(account = AccountOkex("anta_anta001@pt_okex_btc"))

# u = UpdateGateWallet("/Users/chelseyshao/.cr_assis")
# ret = u.send_requests()
# def gen_sign(method, url, query_string=None, payload_string=None):
#     key =  "3da6aac98115eebc2b5c71fcc39a4293"      # api_key
#     secret = "c8ae2bcef4056ee142b67e8ad782361e1d6c264393587191f2252ab57204e25f"     # api_secret
#     t = time.time()
#     m = hashlib.sha512()
#     m.update((payload_string or "").encode('utf-8'))
#     hashed_payload = m.hexdigest()
#     s = '%s\n%s\n%s\n%s\n%s' % (method, url, query_string or "", hashed_payload, t)
#     sign = hmac.new(secret.encode('utf-8'), s.encode('utf-8'), hashlib.sha512).hexdigest()
#     return {'KEY': key, 'Timestamp': str(t), 'SIGN': sign}

# def get_wallet_balance():
#     host = "https://api.gateio.ws"
#     prefix = "/api/v4"
#     headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
#     url = '/wallet/total_balance'
#     query_param = ''
#     # for `gen_sign` implementation, refer to section `Authentication` above
#     sign_headers = gen_sign('GET', prefix + url, query_param)
#     headers.update(sign_headers)
#     r = requests.request('GET', host + prefix + url, headers=headers)
#     print(r.json())

# a = get_wallet_balance()


# u = UpdateOkexMarket()
# ret = u.update_coin_interest(coin = "XLM")

# api = MarketAPI()
# response = api.get_lending_summary()
# ret = response.json() if response.status_code == 200 else {"data": []}

# api = AccountAPI(name = "hf_okex01")
# response = api.get_bills_details(ccy = "USDT")
# ret = response.json()
api = AccountAPI(name = "anta_anta001")

response = api.get_account_balance()
balance = response.json()
data = balance["data"][0]
assets = {i["ccy"]: float(i["disEq"]) for i in data["details"]}

response = api.get_positions()
ret = response.json()

mm_contract = {}
for info in ret["data"]:
    mm_contract[info["instId"]] = float(info["mmr"]) * float(info["markPx"]) if info["instId"].split("-")[1] == "USD" else float(info["mmr"])
position = ret

response = api.get_position_risk()
ret = response.json()
dataokex = ConnectOkex()
account = AccountOkex("bm_bm001@pt_okex_btc")
liability = pd.DataFrame(columns = ["eq", "mmr", "price", "mm"])
for info in ret["data"][0]["balData"]:
    eq = float(info["eq"])
    if eq < 0:
        ccy = info["ccy"]
        mmr = dataokex.get_mmr_spot(ccy, amount = -eq)
        price = account.get_coin_price(ccy) if ccy not in ["USDT", "BUSD", "USDC"] else 1
        mm_ = -eq * mmr * price
        liability.loc[ccy] = [eq, mmr, price, mm_]

# ret = account.get_account_position()
# position = account.get_now_position().drop(["diff", "diff_U", "is_exposure", "usdt"], axis = 1)
# print(position)
# open_price = account.get_open_price().drop("usdt", axis = 1)
# value = (position * open_price).values.sum()
# print(value)

ret = account.get_now_parameter()
print(ret.loc[0, "_comments"]["timestamp"])