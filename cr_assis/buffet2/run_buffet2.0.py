from cr_assis.buffet2.buffetOkex import BuffetOkex
from cr_assis.load import *

def run():
    bft = BuffetOkex()
    bft.json_path = "/home/ssh/jupyter/cr_assis/cr_assis/config/bft2"
    bft.run_buffet(is_save = True, upload= True)

if __name__=='__main__':
    run()