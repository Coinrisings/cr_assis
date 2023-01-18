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
account = AccountBase(deploy_id = "test_otest4@dt_okex_cfuture_okex_uswap_btc")
test_otest4 = AccountData(username = account.username,
                        client = account.client,
                        parameter_name=account.parameter_name,
                        master = account.master,
                        slave=account.slave,
                        principal_currency="BTC",
                        strategy=account.strategy,
                        deploy_id=account.deploy_id)
# spreads = test_otest4.get_spreads(coin = "btc", suffix = "230331")
buffet2_=buffet(accounts = [test_otest4])
all_parameter=buffet2_.get_parameter(com='okx_usd_future-okx_usdt_swap',accounts=[],ingore_account=[],upload=True)