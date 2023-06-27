import datetime, datetime, time
import pandas as pd
import numpy as np
from cr_assis.account.accountOkex import AccountOkex
from research.account.accountBinance import AccountBinance
from cr_assis.connect.connectOkex import ConnectOkex
from cr_assis.api.okex.marketApi import MarketAPI
from cr_assis.api.okex.accountApi import AccountAPI
from cr_assis.pnl.okexPnl import OkexPnl
from cr_assis.pnl.binancePnl import BinancePnl
from cr_assis.draw import draw_ssh
from bokeh.models import NumeralTickFormatter
from bokeh.plotting import show

# pnl = OkexPnl()
# rate = pnl.get_rate(deploy_id = "test_otest5@pt_okex_btc", start = datetime.datetime(2023,6,22,0,0,0), end = datetime.datetime(2023,6,26,0,0,0))


pnl = BinancePnl()
# mv = pnl.get_mv(coin = "btc", name = "test_lxy003", start = datetime.datetime(2023,6,22,0,0,0), end = datetime.datetime(2023,6,27,0,0,0))
df = pnl.get_rate(deploy_id= "test_lxy003@dt_binance_cswap_binance_uswap_btc", start = datetime.datetime(2023,6,22,0,0,0), end = datetime.datetime(2023,6,27,0,0,0))

pnl = OkexPnl()
rate = pnl.get_rate(deploy_id = "test_otest5@pt_okex_btc", start = datetime.datetime(2023,6,22,0,0,0), end = datetime.datetime(2023,6,26,0,0,0))
df = pnl.get_long_bills(name = "test_otest5", start = datetime.datetime(2023,6,22,0,0,0), end = datetime.datetime.now())
ret = pnl.handle_bills(df, is_play=False)
ret = pnl.get_slip(name = "test_otest5", start = datetime.datetime(2023,6,22,0,0,0), end = datetime.datetime(2023,6,26,0,0,0), is_play= False)
result1 = pnl.bills_data[["dt", "cum_pnl", "fake_cum_pnl"]].copy()
result1.set_index("dt",inplace=True)
result2 = pnl.slip[["dt", "slip_page"]].copy()
result2.set_index("dt", inplace=True)
result = pd.merge(result1, result2, left_index = True, right_index=True, how="outer")
result = result.fillna(method = "ffill")
p = draw_ssh.line_doubleY(result, right_columns=["slip_page"],play=False)
p.yaxis[1].formatter = NumeralTickFormatter(format="0.0000%")
show(p)

account = AccountOkex(deploy_id="bm_bm001@pt_okex_btc")
account.get_account_position()
ret = account.get_now_parameter()
print(ret.loc[0, "_comments"]["timestamp"])
position = account.get_now_position().drop(["diff", "diff_U", "is_exposure", "usdt"], axis = 1)
print(position)
open_price = account.get_open_price().drop("usdt", axis = 1)
value = (position * open_price).values.sum()
print(value)


api = AccountAPI()
api.name = "hf_okex01"
api.load_account_api()
end = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)
start = int(datetime.datetime.timestamp(datetime.datetime.now() + datetime.timedelta(days = -90)) * 1000)
ts = end
data = []
while ts >= start:
    response = api.get_bills_details(end=ts)
    if response.status_code == 200:
        ret = response.json()["data"]
        data += ret
        ts = int(ret[-1]["ts"])
    elif response.status_code == 429:
        time.sleep(0.1)
    else:
        print(response.status_code)
        print(response.json())
        break
df = pd.DataFrame(data)
df["dt"] = df["ts"].apply(lambda x: datetime.datetime.fromtimestamp(float(x)/1000))
df.sort_values(by = "dt", inplace = True)
df.set_index("dt", inplace = True)
api = AccountAPI()
api.name = "hf_gate03"
api.load_account_api()
response = api.get_futures_usdt_balance()
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