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
from cr_assis.eva.evaGateWalletNew import EvaGateWallet
gate = EvaGateWallet()
start = datetime.datetime(2023,7,5,0,0,0)
end = datetime.datetime(2023,7,17,17,0,0)
gate.read_total_summary(start, end, accounts = [])

account = AccountOkex(deploy_id="test_hfok01@pt_okex_btc")
account.get_account_position()
position = account.get_now_position().drop(["diff", "diff_U", "is_exposure", "usdt"], axis = 1)
print(position)
open_price = account.get_open_price().drop("usdt", axis = 1)
value = (position * open_price).values.sum()
print(value)

# pnl = OkexPnl()
# rate = pnl.get_rate(deploy_id = "test_otest5@pt_okex_btc", start = datetime.datetime(2023,6,22,0,0,0), end = datetime.datetime(2023,6,26,0,0,0))


# pnl = BinancePnl()
# # mv = pnl.get_mv(coin = "btc", name = "test_lxy003", start = datetime.datetime(2023,6,22,0,0,0), end = datetime.datetime(2023,6,27,0,0,0))
# df = pnl.get_rate(deploy_id= "test_lxy003@dt_binance_cswap_binance_uswap_btc", start = datetime.datetime(2023,6,22,0,0,0), end = datetime.datetime(2023,6,27,0,0,0))

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



