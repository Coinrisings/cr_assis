from accountBase import AccountBase
from research.utils.ObjectDataType import AccountData
from buffet2 import Get_Parameter as buffet
import os, yaml
with open(f"{os.environ['HOME']}/.cryptobridge/private_key.yml", "rb") as f:
    data = yaml.load(f, Loader= yaml.SafeLoader)
for info in data:
    if "mongo" in info.keys():
        os.environ["MONGO_URI"] = info['mongo']
        os.environ["INFLUX_URI"] = info['influx']
        os.environ["INFLUX_MARKET_URI"] = info['influx_market']
account = AccountBase(deploy_id = "bg_bg003@dt_okex_uswap_okex_cfuture_btc")
bg_bg003 = AccountData(username = account.username,
                        client = account.client,
                        parameter_name=account.parameter_name,
                        master = account.master,
                        slave= account.slave,
                        principal_currency="BTC",
                        strategy= account.strategy,
                        deploy_id= account.deploy_id)
otest4 = AccountData(username = "otest4",
                        client = "test",
                        parameter_name= "test_otest4",
                        master = account.master,
                        slave= account.slave,
                        principal_currency="BTC",
                        strategy= account.strategy,
                        deploy_id= "test_otest4@dt_okex_uswap_okex_cfuture_btc")
buffet2_=buffet(accounts = [bg_bg003, otest4])
all_parameter=buffet2_.get_parameter(com='okx_usdt_swap-okx_usd_future',accounts=[],ingore_account=[],upload= False)