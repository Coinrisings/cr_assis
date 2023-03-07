import os, datetime
import pandas as pd
from research.utils import draw_ssh
from bokeh.plotting import show
from bokeh.models.widgets import Panel, Tabs
from bokeh.plotting import output_file
from bokeh.io import save

file_path = "/Users/ssh/Documents/MEGA/SSH/coinrising/DT/others/"
funding = pd.read_csv(f"{file_path}funding.csv", index_col= 0)
funding["dt"] = funding.index
funding.index = funding["dt"].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
klines = {}
names = ["BTC-USDT-INDEX", "BTC-USD-INDEX", "BTC-USDT-SWAP", "BTC-USD-SWAP"]
price = pd.DataFrame(columns = names)
for name in names:
    data = pd.read_csv(f"{file_path}{name}.csv", index_col=0)
    data.index = data['dt'].apply(lambda x: datetime.datetime.strptime(x[:19], "%Y-%m-%d %H:%M:%S"))
    klines[name] = data.copy()
    price[name] = data['close']
price["USD"] = price["BTC-USD-SWAP"] / price["BTC-USD-INDEX"] - 1
price["USDT"] = price["BTC-USDT-SWAP"] / price["BTC-USDT-INDEX"] - 1
price["SWAP"] = price["BTC-USD-SWAP"] / price["BTC-USDT-SWAP"] - 1
price["INDEX"] = price["BTC-USD-INDEX"] / price["BTC-USDT-INDEX"] - 1

tabs = []
# play USD-USDT
result = pd.DataFrame(columns = ["USD-USDT", "funding"])
result["USD-USDT"] = (price["USD"] - price["USDT"]).resample('8h',label='right').mean()
result["funding"] = funding["BTC"]
p1 = draw_ssh.line(result.cumsum(), title = "USD-USDT And Funding", play = False)
tab = Panel(child = p1, title = "USD-USDT And Funding")
tabs.append(tab)

#play USD, USDT and funding
result = pd.DataFrame(columns = ["USD", "USDT", "funding"])
result["USD"] = price["USD"].resample('8h',label='right').mean()
result["USDT"] = price["USDT"].resample('8h',label='right').mean()
result["funding"] = funding["BTC"]
p2 = draw_ssh.line_doubleY(result.cumsum(), right_columns=["funding"],title = "USD, USDT And Funding", play = False)
tab = Panel(child = p2, title = "USD, USDT And Funding")
tabs.append(tab)

#play USD, USDT and price
result = pd.DataFrame(columns = ["USD", "USDT", "price"])
result["USD"] = price["USD"].resample('8h',label='right').mean()
result["USDT"] = price["USDT"].resample('8h',label='right').mean()
result = result.cumsum()
result["price"] = price["BTC-USD-INDEX"].resample('8h',label='right').mean()
p3 = draw_ssh.line_doubleY(result, right_columns = ["price"], title = "USD, USDT And Close", play = False)
tab = Panel(child = p3, title = "USD, USDT And Close")
tabs.append(tab)

t = Tabs(tabs = tabs)
show(t)
output_file(f"{file_path}dto_research.html")
save(t)
print(111111)