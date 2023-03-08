from cr_assis.account.accountBase import AccountBase
from research.utils.ObjectDataType import AccountData
from run.buffet2 import Get_Parameter as buffet
import os, yaml
with open(f"{os.environ['HOME']}/.cryptobridge/private_key.yml", "rb") as f:
    data = yaml.load(f, Loader= yaml.SafeLoader)
for info in data:
    if "mongo" in info.keys():
        os.environ["MONGO_URI"] = info['mongo']
        os.environ["INFLUX_URI"] = info['influx']
        os.environ["INFLUX_MARKET_URI"] = info['influx_market']
bg001 = AccountBase(deploy_id= "bg_001@dt_okex_cfuture_okex_uswap_btc")
bg_001 = AccountData(username = bg001.username,
                        client = bg001.client,
                        parameter_name=bg001.parameter_name,
                        master = bg001.master,
                        slave=bg001.slave,
                        principal_currency="BTC",
                        strategy=bg001.strategy,
                        deploy_id=bg001.deploy_id)
ljw_002 = AccountData(username = "002",
                        client = "ljw",
                        parameter_name="ljw_002",
                        master = "okx_usd_future",
                        slave= "okx_usdt_swap",
                        principal_currency="BTC",
                        strategy="funding",
                        deploy_id="ljw_002@dt_okex_cfuture_okex_uswap_btc")
ch_ch002 = AccountData(username = "ch002",
                        client = "ch",
                        parameter_name="ch_ch002",
                        master = "okx_usd_future",
                        slave= "okx_usdt_swap",
                        principal_currency="BTC",
                        strategy="funding",
                        deploy_id="ch_ch002@dt_okex_cfuture_okex_uswap_btc")
ch_ch009 = AccountData(username = "ch009",
                        client = "ch",
                        parameter_name="ch_ch009",
                        master = "okx_usd_future",
                        slave= "okx_usdt_swap",
                        principal_currency="BTC",
                        strategy="funding",
                        deploy_id="ch_ch009@dt_okex_cfuture_okex_uswap_btc")


account = AccountBase(deploy_id = "bg_bg003@dt_okex_uswap_okex_cfuture_btc")
bg_bg003 = AccountData(username = account.username,
                        client = account.client,
                        parameter_name=account.parameter_name,
                        master = account.master,
                        slave= account.slave,
                        principal_currency="BTC",
                        strategy= account.strategy,
                        deploy_id= account.deploy_id)
bm_bm001 = AccountData(username = "bm001",
                        client = "bm",
                        parameter_name= "bm_bm001",
                        master = account.master,
                        slave= account.slave,
                        principal_currency="BTC",
                        strategy= account.strategy,
                        deploy_id= "bm_bm001@dt_okex_uswap_okex_cfuture_btc")
ljw_001 = AccountData(username = "001",
                        client = "ljw",
                        parameter_name="ljw_001",
                        master = "okx_usdt_swap",
                        slave= "okx_usd_future",
                        principal_currency="BTC",
                        strategy="funding",
                        deploy_id="ljw_001@dt_okex_uswap_okex_cfuture_btc")
ch_ch003 = AccountData(username = "ch003",
                        client = "ch",
                        parameter_name="ch_ch003",
                        master = "okx_usdt_swap",
                        slave= "okx_usd_future",
                        principal_currency="BTC",
                        strategy="funding",
                        deploy_id="ch_ch003@dt_okex_uswap_okex_cfuture_btc")
ch_ch004 = AccountData(username = "ch004",
                        client = "ch",
                        parameter_name="ch_ch004",
                        master = "okx_usdt_swap",
                        slave= "okx_usd_future",
                        principal_currency="BTC",
                        strategy="funding",
                        deploy_id="ch_ch004@dt_okex_uswap_okex_cfuture_btc")

otest8 = AccountData(username = "otest8",
                        client = "test",
                        parameter_name= "test_otest8",
                        master = "okx_usd_future",
                        slave= "okx_usd_swap",
                        principal_currency="BTC",
                        strategy= "funding",
                        deploy_id= "test_otest8@dt_okex_cfuture_okex_cswap_btc")
# spreads = test_otest4.get_spreads(coin = "btc", suffix = "230331")
buffet2_=buffet(accounts = [bg_001, ch_ch009, ljw_002])
all_parameter=buffet2_.get_parameter(com='okx_usd_future-okx_usdt_swap',accounts=[],ingore_account=[],upload= True)

buffet2_=buffet(accounts = [bg_bg003, bm_bm001, ljw_001, ch_ch003, ch_ch004])
all_parameter=buffet2_.get_parameter(com='okx_usdt_swap-okx_usd_future',accounts=[],ingore_account=[],upload= True)