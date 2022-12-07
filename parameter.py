from accountBase import AccountBase
import pandas as pd
import datetime, os
from github import Github

otest8 = AccountBase(deploy_id = "test_otest8@h3f_okex_uswap_okex_uswap_u")
ht001 = AccountBase(deploy_id = "ht_ht001@dt_okex_cswap_okex_uswap_btc")
# file_path = "/Users/ssh/Documents/MEGA/SSH/coinrising/DT/parameter_reverse/2022-12-05-1"
file_path = "/Users/ssh/Documents/MEGA/SSH/coinrising/BUO/parameter/2022-12-06-1"
if not os.path.exists(file_path):
    os.makedirs(file_path)
account = otest8
cols = ["account", "contract", "portfolio_level", "open", "closemaker", "position", "closetaker","open2", "closemaker2","position2",
	"closetaker2", "fragment", "fragment_min", "funding_stop_open", "funding_stop_close", "Position_multiple", "timestamp",
	"is_long", "chase_tick", "master_pair", "slave_pair"]
parameter = pd.DataFrame(columns = cols)
account.get_account_position()
coin = "btc"
folder = "h3f"
master_pair = "-usdc-swap"
slave_pair = "-usdt-swap"
git_file = "parameter_buo"
level = 1

uplimit = 1
open1 = 0.9993
cm = 1.005
ct = cm + 0.002
open2 = open1 + 1
cm2 = cm - 0.0005
ct2 = ct - 0.0005
is_long = 1
fragment = 1000
fragment_min = 10
if master_pair != "-usd-swap":
    price = account.get_coin_price(coin)
else:
    if coin == "btc":
        price = 100
    else:
        price = 10
position = account.adjEq * uplimit / price
holding_position = float(account.position[account.position["coin"] == "btc"].position.values[-1]) if hasattr(account, "position") and "btc" in account.position.coin.values else 0
position2 = max(position, holding_position) * 2
parameter.loc[0] = [account.parameter_name, coin + master_pair, level, open1, cm, position, ct, open2, cm2,position2, ct2, fragment / price, fragment_min / price, 0.0002, 0.005, 1, datetime.datetime.now() + datetime.timedelta(minutes=5), is_long ,1, coin+master_pair, coin+slave_pair]
parameter = parameter.set_index("account")
parameter.to_excel(f"{file_path}/parameter.xlsx", sheet_name=account.parameter_name)

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
with open(f"{file_path}/parameter.xlsx", "rb") as f:
	data = f.read()
	name = f"excel/{folder}/{git_file}"+".xlsx"
	repo.create_file(name, f"uploaded by ssh at {datetime.datetime.now()}", data)
	print(f"{name} uploaded")
	print(datetime.datetime.now())