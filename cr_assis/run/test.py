import datetime, time, os
import pandas as pd
import numpy as np
from cr_assis.connect.connectData import ConnectData
from cr_assis.connect.connectOkex import ConnectOkex
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
pnl = OkexPnl()
ret = pnl.get_orders(name = "test_otest2", start = datetime.datetime(2023,7,20,12,0,0),end = datetime.datetime.now())


from cr_assis.pnl.okexEquity import OkexEquity
e = OkexEquity()
e.run_equity(deploy_id="test_otest2@pt_okex_btc", start = datetime.datetime(2023,7,19,12,0,0),end = datetime.datetime.now(), plot_width = 1400, plot_height = 400,)

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


deploy_id = "test_hfok01@pt_okex_btc"
name = deploy_id.split("@")[0]
start = datetime.datetime(2023,6,29,14,0,0)
end = datetime.datetime.now()
pnl = OkexPnl()
# rate = pnl.get_rate(deploy_id = deploy_id, start = start, end = end)
# df = pnl.get_long_bills(name = name, start = start, end = end)
# ret = pnl.handle_bills(df, is_play=False)
ret = pnl.get_slip(name = name, start = start, end = end, is_play= False)
result1 = pnl.bills_data[["dt", "cum_pnl", "fake_cum_pnl"]].copy()
result1.set_index("dt",inplace=True)
result2 = pnl.slip[["dt", "slip_page"]].copy()
result2.set_index("dt", inplace=True)
result = pd.merge(result1, result2, left_index = True, right_index=True, how="outer")
result = result.fillna(method = "ffill")
p = draw_ssh.line_doubleY(result, right_columns=["slip_page"],play=False)
p.yaxis[1].formatter = NumeralTickFormatter(format="0.0000%")
show(p)



