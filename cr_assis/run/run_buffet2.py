from cr_assis.account.accountBase import AccountBase
from cr_assis.run.buffet2 import Get_Parameter as buffet
from cr_assis.load import *
bg003 = AccountBase(deploy_id= "bg_bg003@dt_okex_cswap_okex_uswap_btc", is_usdc= True)
ht001 = AccountBase(deploy_id= "ht_ht001@dt_okex_cswap_okex_uswap_btc", is_usdc= True)
anta001 = AccountBase(deploy_id= "anta_anta001@dt_okex_cswap_okex_uswap_btc", is_usdc= True)
bft=buffet(accounts = [bg003, ht001, anta001])
all_parameter = bft.get_parameter(com=bg003.combo,accounts=[],ingore_account=[],upload= True)
all_parameter