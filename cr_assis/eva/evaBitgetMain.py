from cr_assis.eva.evaGateWallet import EvaGateWallet

class EvaBitGetMain(EvaGateWallet):
    def __init__(self):
        super().__init__()
        self.file_path = "/mnt/efs/fs1/data_ssh/mm/capital/gate"