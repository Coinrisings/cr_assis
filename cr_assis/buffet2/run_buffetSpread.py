from cr_assis.buffet2.buffetOkexSpread import BuffetOkexSpread
from cr_assis.load import *

bft = BuffetOkexSpread()
bft.json_path = "/home/ssh/jupyter/cr_assis/cr_assis/config/buffet2_config/pt"
bft.run_buffet(is_save = True, upload= True)