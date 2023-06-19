from cr_assis.buffet2.buffetOkexSpread import BuffetOkexSpread
from cr_assis.load import *

bft = BuffetOkexSpread()
bft.json_path = "/Users/chelseyshao/Documents/GitHub/cr_assis/cr_assis/config/buffet2_config/pt"
bft.run_buffet(is_save = True, upload= True)