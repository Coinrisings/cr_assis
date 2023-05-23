import datetime, requests, datetime, json, hashlib, hmac, base64
import pandas as pd
import numpy as np
from cr_assis.account.accountOkex import AccountOkex

account = AccountOkex(deploy_id="test_otest5@pt_okex_btc")
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
