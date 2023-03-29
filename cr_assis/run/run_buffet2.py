from cr_assis.account.accountBase import AccountBase
from cr_assis.run.buffet2 import Get_Parameter as buffet
from cr_assis.load import *
bg003 = AccountBase(deploy_id= "bg_bg003@dt_okex_cswap_okex_uswap_btc", is_usdc= True)
bft=buffet(accounts = [bg003])
all_parameter = bft.get_parameter(com=bg003.combo,accounts=[],ingore_account=[],upload= False)
all_parameter