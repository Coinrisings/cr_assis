from cr_assis.account.accountBase import AccountBase
import pandas as pd
import numpy as np
import datetime, os, json
from github import Github

# anta001 = AccountBase(deploy_id='anta_anta001@dt_okex_uswap_okex_cfuture_btc')
# bg003 = AccountBase(deploy_id= "bg_bg003@ssf_okexv5_spot_okexv5_uswap_btc")
# ch003 = AccountBase(deploy_id= "ch_ch003@ssf_okexv5_spot_okexv5_uswap_btc")
# ch004 = AccountBase(deploy_id= "ch_ch004@ssf_okexv5_spot_okexv5_uswap_btc")
# ljw002 = AccountBase(deploy_id= "ljw_002@ssf_okexv5_spot_okexv5_uswap_btc")
cr003 = AccountBase(deploy_id = "cr_cr003@dt_okex_cswap_okex_uswap_btc")
ch003 = AccountBase(deploy_id = "ch_ch003@dt_okex_cswap_okex_uswap_btc")
ch004 = AccountBase(deploy_id = "ch_ch004@dt_okex_cswap_okex_uswap_btc")
otest5 = AccountBase(deploy_id = "test_otest5@dt_okex_cswap_okex_uswap_btc")
file_path = f"/Users/ssh/Documents/MEGA/SSH/coinrising/DT/parameter_reverse/{datetime.date.today()}"
# file_path = f"/Users/ssh/Documents/MEGA/SSH/coinrising/DT/parameter_future/{datetime.date.today()}"
# file_path = f"/Users/ssh/Documents/MEGA/SSH/coinrising/BUO/parameter/{datetime.date.today()}"
# file_path = f"/Users/ssh/Documents/MEGA/SSH/coinrising/SSFO/parameter/{datetime.date.today()}"
if not os.path.exists(file_path):
    os.makedirs(file_path)
accounts = [otest5]
cols = ["account", "contract", "portfolio_level", "open", "closemaker", "position", "closetaker","open2", "closemaker2","position2",
	"closetaker2", "fragment", "fragment_min", "funding_stop_open", "funding_stop_close", "Position_multiple", "timestamp",
	"is_long", "chase_tick", "master_pair", "slave_pair"]
parameters = {}
git_file = "parameter_dt"
# git_file = "parameter_ssfo"
local_file = f"parameter_{datetime.datetime.now()}"
suffix = "230331"
num = 0
hours = 2
add = 0
fragment = 2000
fragment_min = 100
loss_open = 0.0005
profit_close = 0.005
closemaker = 1.005
with open("/Users/ssh/Documents/GitHub/cr_assis/cr_assis/config/parameter.json", "r") as f:
    portfolio = json.load(f)
for account in accounts:
    parameter = pd.DataFrame(columns = cols)
    account.get_account_position()
    holding_position = account.position if hasattr(account, "position") else pd.DataFrame(columns = ["coin", "side", "position", "MV", "MV%"])
    holding_position.set_index("coin", inplace= True)
    folder = account.folder
    master_pair = account.contract_master.replace("future", suffix)
    slave_pair = account.contract_slave.replace("future", suffix)
    for coin in set(holding_position.index.values) | set(portfolio.keys()):
        if coin not in holding_position.index.values and account.parameter_name not in portfolio[coin]["accounts"]:
            continue
        level = portfolio[coin]["level"] if coin in portfolio.keys() and account.parameter_name in portfolio[coin]["accounts"] else 0
        side = holding_position.loc[coin, "side"] if coin in holding_position.index.values else ""
        open1 = 2
        cm = closemaker
        if level == 1 and account.parameter_name in portfolio[coin]["accounts"]:
            if coin in holding_position["position"].values and side != holding_position.loc[coin, "side"]:
                pass
            else:
                spreads = account.get_spreads(coin = coin, hours = hours)
                open1 = np.mean(spreads["bid0_spread"]) + add if portfolio[coin]["side"] == "long" else np.mean(spreads["ask0_spread"]) + add
        elif level == -1 and account.parameter_name in portfolio[coin]["accounts"] and coin in holding_position.index.values and side == portfolio[coin]["side"]:
            spreads = account.get_spreads(coin = coin, hours = hours)
            cm = np.mean(spreads["ask0_spread"]) + add if portfolio[coin]["side"] == "long" else np.mean(spreads["bid0_spread"]) + add
        ct = cm + 0.002
        open2 = open1 + 1
        cm2 = cm - 0.0005
        ct2 = ct - 0.0005
        real_side = holding_position.loc[coin, "side"] if coin in holding_position.index.values else portfolio[coin]["side"]
        is_long = 1 if real_side == "long" else 0
        if master_pair.split("-")[1] != "usd":
            price = account.get_coin_price(coin)
        else:
            if coin == "btc":
                price = 100
            else:
                price = 10
        open_position = holding_position.loc[coin, "position"] if coin in holding_position.index.values else 0
        uplimit = portfolio[coin]["uplimit"] if coin in portfolio.keys() else 0
        expect_position = account.adjEq * uplimit / price
        position2 = max(open_position, expect_position) * 2
        position = position2 / 2
        contract = coin + master_pair
        parameter.loc[num] = [account.parameter_name, contract, level, open1, cm, position, ct, open2, cm2, position2, ct2, fragment / price, fragment_min / price, loss_open, profit_close, 1, datetime.datetime.now() + datetime.timedelta(minutes=3), is_long ,1, coin+master_pair, coin+slave_pair]
        num += 1
    parameter["timestamp"] = datetime.datetime.now() + datetime.timedelta(minutes=3)
    parameter = parameter.set_index("account")
    parameters[account.parameter_name] = parameter.copy()
writer = pd.ExcelWriter(f"{file_path}/{local_file}.xlsx", engine='openpyxl')
for sheet_name in parameters.keys():
    data = parameters[sheet_name]
    data["timestamp"] = datetime.datetime.now() + datetime.timedelta(minutes=3)
    data.to_excel(excel_writer=writer, sheet_name=sheet_name, encoding="UTF-8")
writer.save()
writer.close()

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