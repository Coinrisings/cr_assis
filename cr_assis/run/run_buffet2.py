from cr_assis.account.accountBase import AccountBase
from cr_assis.run.buffet2 import Get_Parameter as buffet
import os, yaml
otest5 = AccountBase(deploy_id= "test_otest5@dt_okex_cswap_okex_uswap_btc", is_usdc= True)
bft=buffet(accounts = [otest5])
all_parameter = bft.get_parameter(com=otest5.combo,accounts=[],ingore_account=[],upload= True)
all_parameter