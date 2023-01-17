from accountBase import AccountBase
import pandas as pd
import datetime, os
from github import Github

otest8 = AccountBase(deploy_id = "test_otest8@h3f_okex_uswap_okex_uswap_btc")
otest4 = AccountBase(deploy_id = "test_otest4@dt_okex_cfuture_okex_uswap_btc")
file_path = f"/Users/ssh/Documents/MEGA/SSH/coinrising/DT/parameter_future/{datetime.date.today()}-1"
# file_path = f"/Users/ssh/Documents/MEGA/SSH/coinrising/BUO/parameter/{datetime.date.today()}-1"
if not os.path.exists(file_path):
    os.makedirs(file_path)
account = otest4
cols = ["account", "contract", "portfolio_level", "open", "closemaker", "position", "closetaker","open2", "closemaker2","position2",
	"closetaker2", "fragment", "fragment_min", "funding_stop_open", "funding_stop_close", "Position_multiple", "timestamp",
	"is_long", "chase_tick", "master_pair", "slave_pair"]
parameter = pd.DataFrame(columns = cols)
account.get_account_position()

coin = "btc"
suffix = "230301"
folder = account.folder
master_pair = account.contract_master.replace("future", "230331")
slave_pair = account.contract_slave.replace("future", "230331")
git_file = "parameter_dt_future"
local_file = f"parameter_{datetime.datetime.now()}"
level = 1
uplimit = 1.25
open1 = 0.9995
cm = 1.005
ct = cm + 0.002
open2 = open1 + 1
cm2 = cm - 0.0005
ct2 = ct - 0.0005
is_long = 0
fragment = 6000
fragment_min = 10
if master_pair.split("-")[1] != "usd":
    price = account.get_coin_price(coin)
else:
    if coin == "btc":
        price = 100
    else:
        price = 10
position = account.adjEq * uplimit / price
# holding_position = 0
holding_position = float(account.position[account.position["coin"] == "btc"].position.values[-1]) if hasattr(account, "position") and "btc" in account.position.coin.values else 0
position2 = max(position, holding_position) * 2
parameter.loc[0] = [account.parameter_name, coin + master_pair, level, open1, cm, position, ct, open2, cm2,position2, ct2, fragment / price, fragment_min / price, 0.0002, 0.005, 1, datetime.datetime.now() + datetime.timedelta(minutes=5), is_long ,1, coin+master_pair, coin+slave_pair]
parameter = parameter.set_index("account")
parameter.to_excel(f"{file_path}/{local_file}.xlsx", sheet_name=account.parameter_name)
#upload
with open(f"{os.environ['HOME']}/.git-credentials", "r") as f:
    data = f.read()

access_token = data.split(":")[-1].split("@")[0]
g = Github(login_or_token= access_token)
repo = g.get_repo("Coinrisings/parameters")
contents = repo.get_contents(f"excel/{folder}")
for content_file in contents:
    if git_file in content_file.name:
        repo.delete_file(content_file.path, message = f"ssh removes {content_file.name} at {datetime.datetime.now()}", sha = content_file.sha)
        print(f"ssh removes {content_file.name} at {datetime.datetime.now()}")
with open(f"{file_path}/{local_file}.xlsx", "rb") as f:
	data = f.read()
	name = f"excel/{folder}/{git_file}"+".xlsx"
	repo.create_file(name, f"uploaded by ssh at {datetime.datetime.now()}", data)
	print(f"{name} uploaded")
	print(datetime.datetime.now())