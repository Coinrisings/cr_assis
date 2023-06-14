import datetime, requests, datetime, json, os
import pandas as pd
import numpy as np
from cr_assis.account.accountOkex import AccountOkex
from cr_assis.account.accountBinance import AccountBinance
from cr_assis.connect.connectOkex import ConnectOkex
from cr_assis.connect.updateOkexMarket import UpdateOkexMarket
from cr_assis.api.okex.marketApi import MarketAPI
from cr_assis.api.okex.publicApi import PublicAPI
u = UpdateOkexMarket()
ret = u.update_coin_interest(coin = "XLM")

api = MarketAPI()
response = api.get_lending_summary()
ret = response.json() if response.status_code == 200 else {"data": []}

sql = f"/api/v5/account/balance"
secret = "F1CDA54959C8CA368E8FE00701CE5CAF"
api_key = "b54d6744-6eb2-4683-9e3f-cc08df499fbd"
passphrase = "2tOs1I7cn1gR8Ft"
response = dataokex.handle_account_get_query(sql, secret, api_key, passphrase)
ret = response.json()
data = ret["data"][0]
adjEq = float(data["adjEq"])
mm = float(data["mmr"])
mr = float(data["mgnRatio"])
print(f"adjEq: {adjEq}")
print(f"mm: {mm}")
print(f"mr: {mr}")

sql = f"/api/v5/account/positions"
response = dataokex.handle_account_get_query(sql, secret, api_key, passphrase)
ret = response.json()

mm_contract = {}
for info in ret["data"]:
    mm_contract[info["instId"]] = float(info["mmr"]) * float(info["markPx"]) if info["instId"].split("-")[1] == "USD" else float(info["mmr"])
position = ret

sql = f"/api/v5/account/account-position-risk"
response = dataokex.handle_account_get_query(sql, secret, api_key, passphrase)
ret = response.json()
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