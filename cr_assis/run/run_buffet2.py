from cr_assis.account.accountBase import AccountBase
from cr_assis.run.buffet2 import Get_Parameter as buffet
from cr_assis.load import *
bg003 = AccountBase(deploy_id= "bg_bg003@dt_okex_cswap_okex_uswap_btc", is_usdc= True)
bm001 = AccountBase(deploy_id= "bm_bm001@dt_okex_cswap_okex_uswap_btc", is_usdc= True)
bg001 = AccountBase(deploy_id= "bg_001@dt_okex_cswap_okex_uswap_btc", is_usdc= True)
ljw001 = AccountBase(deploy_id= "ljw_001@dt_okex_cswap_okex_uswap_btc", is_usdc= True)
ljw002 = AccountBase(deploy_id= "ljw_002@dt_okex_cswap_okex_uswap_btc", is_usdc= True)
cr001 = AccountBase(deploy_id= "cr_cr001@dt_okex_cswap_okex_uswap_btc", is_usdc= True)
bft=buffet(accounts = [bg003, bm001, ljw001, cr001, bg001, ljw002])
all_parameter = bft.get_parameter(com=bg003.combo,accounts=[],ingore_account=[],upload= True)