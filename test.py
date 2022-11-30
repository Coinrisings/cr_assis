from accountBase import AccountBase
import pandas as pd
import datetime, os
from github import Github

wz001 = AccountBase(deploy_id = "wz_001@dt_okex_cswap_okex_uswap_usdt")
cols = ["account", "contract", "portfolio_level", "open", "closemaker", "position", "closetaker","open2", "closemaker2","position2",
        "closetaker2", "fragment", "fragment_min", "funding_stop_open", "funding_stop_close", "Position_multiple", "timestamp",
        "is_long", "chase_tick"]
parameter = pd.DataFrame(columns = cols)
wz001.get_account_position()
position = wz001.adjEq * 1.5 / 100
parameter.loc[0] = [wz001.parameter_name, "btc-usd-swap", 1, 1.000371425596116, 1.005, position, 1.007, 2, 1.0045,position*2,1.0065, 600, 1, 0.0001, 0.001, 1, datetime.datetime.now() + datetime.timedelta(minutes=5), 1 ,1]
parameter = parameter.set_index("account")
parameter.to_excel("/Users/ssh/Documents/MEGA/SSH/coinrising/DT/parameter_reverse/2022-11-29-1/dt_parameter.xlsx", sheet_name=wz001.parameter_name)

#upload
with open(f"{os.environ['HOME']}/.git-credentials", "r") as f:
        data = f.read()
access_token = data.split(":")[-1].split("@")[0]
g = Github(login_or_token= access_token)
repo = g.get_repo("Coinrisings/parameters")
os.chdir("/Users/ssh/Documents/MEGA/SSH/coinrising/DT/parameter_reverse/2022-11-29-1")
contents = repo.get_contents("excel/dt")
for content_file in contents:
    if "parameter_dt_reverse" in content_file.name:
        repo.delete_file(content_file.path, message = f"ssh removes {content_file.name} at {datetime.datetime.now()}", sha = content_file.sha)
        print(f"ssh removes {content_file.name} at {datetime.datetime.now()}")
with open("dt_parameter.xlsx", "rb") as f:
        data = f.read()
        name = "excel/dt/"+"parameter_dt_reverse.xlsx"
        repo.create_file(name, f"uploaded by ssh at {datetime.datetime.now()}", data)
        print(f"{name} uploaded")
        print(datetime.datetime.now())