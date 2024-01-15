class GeneralUTXO:
    def __init__(self, utxoId, txHash, txType, addresses, value):
        self.utxoId = utxoId
        self.txHash = txHash
        self.txType = txType
        self.addresses = addresses 
        self.value = value
        