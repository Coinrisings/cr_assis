import datetime, requests, datetime, json, hashlib, hmac, base64, os
import pandas as pd
import numpy as np
from cr_assis.account.accountOkex import AccountOkex
from cr_assis.account.accountBinance import AccountBinance
from cr_assis.connect.connectOkex import ConnectOkex
dataokex = ConnectOkex()
account = AccountBinance(deploy_id="test_btest6@pt_binance_btc")
account.get_account_position()

with open("/Users/chelseyshao/Documents/GitHub/cr_assis/cr_assis/config/bg003_mr7.json", "r") as f:
    file_data = json.load(f)
mm_contract = {}
for data in file_data:
    info = data["opaque"]
    mm_contract[info["instId"]] = float(info["mmr"]) * float(info["markPx"]) if info["instId"].split("-")[1] == "USD" else float(info["mmr"])
liability = pd.DataFrame(columns = ["eq", "mmr", "price", "mm"])
liability.loc["ADA", ["eq", "mmr", "price"]] = [58230.62279744704, 0.05, 0.31483]
liability.loc["USDT", ["eq", "mmr", "price"]] = [969846.4932436475, 0.03, 1]
liability["mm"] = liability["eq"] * liability["mmr"] * liability["price"]
adjEq = 471387.4038771193
mr = 7.179807593675047
notionalUsd = 2857745.8561015413
mm = adjEq / mr
cal_mm = sum(mm_contract.values()) + liability["mm"].sum() + notionalUsd * 0.00045
print(f"mm: {mm}")
print(f"cal_mm: {cal_mm}")


with open("/Users/chelseyshao/Documents/GitHub/cr_assis/cr_assis/config/bg003.json", "r") as f:
    file_data = json.load(f)
mm_contract = {}
for data in file_data:
    info = data["opaque"]
    mm_contract[info["instId"]] = float(info["mmr"]) * float(info["markPx"]) if info["instId"].split("-")[1] == "USD" else float(info["mmr"])
liability = pd.DataFrame(columns = ["eq", "mmr", "price", "mm"])
liability.loc["ADA", ["eq", "mmr", "price"]] = [52648.373254475155, 0.05, 0.32034]
liability.loc["USDT", ["eq", "mmr", "price"]] = [984586.1990340787, 0.0625, 1]
liability["mm"] = liability["eq"] * liability["mmr"] * liability["price"]
adjEq = 469759.51897605974
mr = 4.737882901502152
notionalUsd = 2886875.3336922787
mm = adjEq / mr
cal_mm = sum(mm_contract.values()) + liability["mm"].sum() + notionalUsd * 0.00045
print(f"mm: {mm}")
print(f"cal_mm: {cal_mm}")

sql = f"/api/v5/account/balance"
secret = "F1CDA54959C8CA368E8FE00701CE5CAF"
api_key = "b54d6744-6eb2-4683-9e3f-cc08df499fbd"
passphrase = "2tOs1I7cn1gR8Ft"
response = dataokex.handle_account_get_equery(sql, secret, api_key, passphrase)
ret = response.json()
data = ret["data"][0]
adjEq = float(data["adjEq"])
mm = float(data["mmr"])
mr = float(data["mgnRatio"])
print(f"adjEq: {adjEq}")
print(f"mm: {mm}")
print(f"mr: {mr}")

sql = f"/api/v5/account/positions"
response = dataokex.handle_account_get_equery(sql, secret, api_key, passphrase)
ret = response.json()

mm_contract = {}
for info in ret["data"]:
    mm_contract[info["instId"]] = float(info["mmr"]) * float(info["markPx"]) if info["instId"].split("-")[1] == "USD" else float(info["mmr"])
position = ret

sql = f"/api/v5/account/account-position-risk"
response = dataokex.handle_account_get_equery(sql, secret, api_key, passphrase)
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
account = AccountOkex(deploy_id="bm_bm001@pt_okex_btc")
# ret = account.get_account_position()
# position = account.get_now_position().drop(["diff", "diff_U", "is_exposure", "usdt"], axis = 1)
# print(position)
# open_price = account.get_open_price().drop("usdt", axis = 1)
# value = (position * open_price).values.sum()
# print(value)

ret = account.get_now_parameter()
print(ret.loc[0, "_comments"]["timestamp"])

ts = round(datetime.datetime.timestamp(datetime.datetime.now() + datetime.timedelta(days = 0)) * 1000, 0)
url = "https://www.okx.com/v3/users/fee/getVolumeLevelInfo?t={ts}"
headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "authorization": "eyJhbGciOiJIUzUxMiJ9.eyJqdGkiOiJleDExMDE2ODIyMzQ5MzM1Mjg5NDJDNDEzQ0UzOENGRkY0MVlkS3ciLCJ1aWQiOiJLTnpIOFl3eDh6RVdIWWhFcGhzckJ3PT0iLCJzdGEiOjAsIm1pZCI6IktOekg4WXd4OHpFV0hZaEVwaHNyQnc9PSIsImlhdCI6MTY4MjIzNDkzMywiZXhwIjoxNjgyODM5NzMzLCJiaWQiOjAsImRvbSI6Ind3dy5va3guY29tIiwiZWlkIjoxLCJpc3MiOiJva2NvaW4iLCJzdWIiOiI4RUYwM0JGMDlGNjAxMjJBOTQzNjA1MUY3OTEzNDk5RSJ9.to7oF7gkwTSk8Mva0eTptfs43NKp3CbJCGXbZdZOoMCqWspU_bxbZEJ4ImZwPtlrpYMrBwQ2Cl1bXxHcHV_YjA"}
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
