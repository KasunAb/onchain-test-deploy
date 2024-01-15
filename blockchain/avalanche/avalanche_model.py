# avalanche_model.py
from src.models.general_blockchain_model import GeneralBlockchainModel

class Avalanche_X_Model(GeneralBlockchainModel):
    def __init__(self, txHash, blockHash, blockHeight, txType, timestamp , memo, chainFormat,  amountUnlocked, amountCreated):
        super().__init__(txHash, blockHash, timestamp, None)
        self.blockHeight = blockHeight
        self.txType = txType
        self.memo = memo
        self.chainFormat = chainFormat
        self.amountUnlocked = amountUnlocked
        self.amountCreated = amountCreated
        
class Avalanche_C_Model(GeneralBlockchainModel):
    def __init__(self, txHash, blockHash, blockHeight, txType, timestamp, sourceChain, destinationChain, memo,  amountUnlocked, amountCreated):
        super().__init__(txHash, blockHash, timestamp, None)
        self.blockHeight = blockHeight
        self.txType = txType
        self.sourceChain = sourceChain
        self.destinationChain = destinationChain
        self.memo = memo
        self.amountUnlocked = amountUnlocked
        self.amountCreated = amountCreated

class Avalanche_P_Model(GeneralBlockchainModel):
    def __init__(self, txHash, txType, blockTimestamp, blockNumber, blockHash, memo, sourceChain, destinationChain, rewardAddresses, estimatedReward, startTimestamp, endTimestamp, delegationFeePercent, nodeId, subnetId, value, amountStaked, amountBurned):
        super().__init__(txHash, blockHash, blockTimestamp, value)
        self.txType = txType
        self.blockNumber = blockNumber
        self.memo = memo
        self.nodeId = nodeId
        self.subnetId = subnetId
        self.amountStaked = amountStaked
        self.amountBurned = amountBurned
        self.sourceChain = sourceChain
        self.destinationChain = destinationChain
        self.rewardAddresses = rewardAddresses
        self.estimatedReward = estimatedReward
        self.startTimestamp = startTimestamp
        self.endTimestamp = endTimestamp
        self.delegationFeePercent = delegationFeePercent

