from accountBase import AccountBase
import pandas as pd
import datetime, os
from github import Github
from IPython.display import display
anta001 = AccountBase(deploy_id = "anta_anta001@dt_okex_cswap_okex_uswap_btc")
bg001 = AccountBase(deploy_id = "bg_001@dt_okex_cswap_okex_uswap_btc")
bg003 = AccountBase(deploy_id = "bg_bg003@dt_okex_cswap_okex_uswap_btc")
ch002 = AccountBase(deploy_id = "ch_ch002@dt_okex_cswap_okex_uswap_btc")
ch003 = AccountBase(deploy_id = "ch_ch003@dt_okex_cswap_okex_uswap_btc")
ch004 = AccountBase(deploy_id = "ch_ch004@dt_okex_cswap_okex_uswap_btc")
ch005 = AccountBase(deploy_id = "ch_ch005@dt_okex_cswap_okex_uswap_btc")
ch006 = AccountBase(deploy_id = "ch_ch006@dt_okex_cswap_okex_uswap_btc")
ch007 = AccountBase(deploy_id = "ch_ch007@dt_okex_cswap_okex_uswap_btc")
ch008 = AccountBase(deploy_id = "ch_ch008@dt_okex_cswap_okex_uswap_btc")
ch009 = AccountBase(deploy_id = "ch_ch009@dt_okex_cswap_okex_uswap_btc")
cr001 = AccountBase(deploy_id = "cr_cr001@dt_okex_cswap_okex_uswap_btc")
ht001 = AccountBase(deploy_id = "ht_ht001@dt_okex_cswap_okex_uswap_btc")
ljw001 = AccountBase(deploy_id = "ljw_001@dt_okex_cswap_okex_uswap_btc")
ljw002 = AccountBase(deploy_id = "ljw_002@dt_okex_cswap_okex_uswap_btc")
wz001 = AccountBase(deploy_id = "wz_001@dt_okex_cswap_okex_uswap_usdt")
accounts = [anta001, bg001, bg003, ch002, ch003, ch004, ch005, ch006, ch007, ch008, ch009, cr001, ht001, ljw001, ljw002, wz001]
# for account in accounts:
#     account.get_account_position()
#     print(account.parameter_name)
#     display(account.position)
file_path = f"/Users/ssh/Documents/MEGA/SSH/coinrising/DT/parameter_reverse/{datetime.date.today()}-1"
# file_path = f"/Users/ssh/Documents/MEGA/SSH/coinrising/BUO/parameter/{datetime.date.today()}-1"
if not os.path.exists(file_path):
    os.makedirs(file_path)
cols = ["account", "contract", "portfolio_level", "open", "closemaker", "position", "closetaker","open2", "closemaker2","position2",
        "closetaker2", "fragment", "fragment_min", "funding_stop_open", "funding_stop_close", "Position_multiple", "timestamp",
        "is_long", "chase_tick", "master_pair", "slave_pair"]
file_name = f"{file_path}/parameter.xlsx"
writer = pd.ExcelWriter(file_name, engine='openpyxl')

for account in accounts:
    parameter = pd.DataFrame(columns = cols)
    account.get_account_position()
    coin = "btc"
    folder = account.folder
    master_pair = account.contract_master
    slave_pair = account.contract_slave
    git_file = "parameter_dt"
    level = 0
    uplimit = 2.5
    open1 = 2
    cm = 2
    ct = 1.003
    open2 = open1 + 1
    cm2 = cm - 0.0003
    ct2 = ct - 0.0003
    is_long = 1
    fragment = 6000
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
    position = position2 / 2
    parameter.loc[0] = [account.parameter_name, coin + master_pair, level, open1, cm, position, ct, open2, cm2,position2, ct2, fragment / price, fragment_min / price, 0.0002, 0.05, 1, datetime.datetime.now() + datetime.timedelta(minutes=5), is_long ,1, coin+master_pair, coin+slave_pair]
    parameter = parameter.set_index("account")
    parameter.to_excel(excel_writer=writer, sheet_name=account.parameter_name, encoding="GBK")
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
with open(f"{file_path}/parameter.xlsx", "rb") as f:
	data = f.read()
	name = f"excel/{folder}/{git_file}"+".xlsx"
	repo.create_file(name, f"uploaded by ssh at {datetime.datetime.now()}", data)
	print(f"{name} uploaded")
	print(datetime.datetime.now())