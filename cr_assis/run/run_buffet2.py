from cr_assis.account.accountBase import AccountBase
from cr_assis.run.buffet2 import Get_Parameter as buffet
from cr_assis.load import *
bft=buffet(accounts = [])
all_parameter = bft.get_parameter(com=bg003.combo,accounts=[],ingore_account=[],upload= False)