import pandas as pd
import requests
import time
from datetime import datetime
import json

import os
import sys
sys.path.insert(0, 'D:\\Academics\\FYP\\Repos\\Blockchain-On-Chain-Extendible-Framework')

from utils.http_utils import fetch_transactions
from blockchain.avalanche.avalanche_model import Avalanche_X_Model, Avalanche_C_Model, Avalanche_P_Model
from blockchain.avalanche.avalanche_UTXO_model import AvalancheUTXO

def extract_x_chain_data(last_timestamp):
    page_token = None

    params = {
        "pageSize": 100
    }

    url = "https://glacier-api.avax.network/v1/networks/mainnet/blockchains/x-chain/transactions"
    
    data = []
    emitted_utxos = []
    consumed_utxos = []
    run = True

    while run:
        
        if page_token:
          params["pageToken"] = page_token

        res_data = fetch_transactions(url, params)
        transactions = res_data.get('transactions', [])

        for tx in transactions:
            timestamp = int(tx.get("timestamp"))
            if timestamp <= last_timestamp:
                run = False
                break
            txHash = tx.get("txHash")
            blockHash = tx.get("blockHash")
            txType=tx.get("txType")
            amount_unlocked, amount_created = calculate_x_transaction_values(tx)

            avalanche_tx = Avalanche_X_Model(
                txHash=txHash,
                blockHash=blockHash,
                blockHeight = tx.get("blockHeight"),
                timestamp=timestamp,
                memo=tx.get("memo"),
                chainFormat=tx.get("chainFormat"),
                txType=txType,
                amountUnlocked = amount_unlocked,
                amountCreated = amount_created
            )
        
            # emmitted UTXOs
            for e_utxo in tx.get('emittedUtxos', []):
                
                asset =  e_utxo['asset']
                
                emit_utxo = AvalancheUTXO(
                        txHash = txHash,
                        txType = txType,
                        blockHash = blockHash,
                        addresses = e_utxo['addresses'],
                        utxoId= e_utxo['utxoId'],
                        assetId = asset.get('assetId'),
                        asset_name = asset.get('name', ''),
                        symbol = asset.get('symbol', ''),
                        denomination = asset.get('denomination',0),
                        asset_type = asset.get('type',''),
                        amount = asset.get('amount',0)
                    )
                
                emitted_utxos.append(emit_utxo.__dict__)

            # consumed UTXOs
            for c_utxo in tx.get('consumedUtxos', []):
                
                asset =  c_utxo['asset']
                
                commit_utxo = AvalancheUTXO(
                        txHash = txHash,
                        txType = txType,
                        blockHash = blockHash,
                        addresses = c_utxo['addresses'],
                        utxoId= c_utxo['utxoId'],
                        assetId = asset.get('assetId'),
                        asset_name = asset.get('name', ''),
                        symbol = asset.get('symbol', ''),
                        denomination = asset.get('denomination',0),
                        asset_type = asset.get('type',''),
                        amount = asset.get('amount',0)
                    )
                consumed_utxos.append(commit_utxo.__dict__)

            page_token = res_data.get('nextPageToken')

            data.append(avalanche_tx.__dict__)
    return pd.DataFrame(data), pd.DataFrame(emitted_utxos), pd.DataFrame(consumed_utxos)



def calculate_x_transaction_values(transaction):
    
    amountUnlocked = transaction.get('amountUnlocked', [])
    amountCreated = transaction.get('amountCreated', [])
    
    amount_unlocked = {}
    amount_created = {}
    
    for amount in amountUnlocked:
        if amount['name'] in amount_unlocked:
            amount_unlocked[amount['name']] += int(amount['amount']) / int(amount['denomination'])
        else:
            amount_unlocked[amount['name']] = int(amount['amount']) / int(amount['denomination'])
    
    for amount in amountCreated:
        if amount['name'] in amount_created:
            amount_created[amount['name']] += int(amount['amount']) / int(amount['denomination'])
        else:
            amount_created[amount['name']] = int(amount['amount']) / int(amount['denomination'])
        
    return amount_unlocked, amount_created


# C chain
def extract_c_chain_data(last_timestamp):
    page_token = None
    
    params = {
        "pageSize": 100
    }

    url = "https://glacier-api.avax.network/v1/networks/mainnet/blockchains/c-chain/transactions"

    data = []
    input_env = []
    output_env = []
    emitted_utxos = []
    consumed_utxos = []
    run = True

    while run:
        if page_token:
            params["pageToken"] = page_token
        
        res_data = fetch_transactions(url, params)
        transactions = res_data.get('transactions', [])

        for tx in transactions:
            
            timestamp=int(tx.get("timestamp"))          
            if timestamp <= last_timestamp:
                run = False
                break

            txHash=tx.get("txHash")
            blockHash=tx.get("blockHash")
            txType=tx.get("txType")
            amount_unlocked, amount_created = calculate_x_transaction_values(tx)

            avalanche_tx = Avalanche_C_Model(
                txHash=txHash,
                blockHash=blockHash,
                blockHeight=tx.get("blockHeight"),
                txType=txType,
                timestamp=timestamp,
                sourceChain=tx.get("sourceChain"),
                destinationChain=tx.get("destinationChain"),
                memo=tx.get("memo"),
                amountUnlocked=amount_unlocked,
                amountCreated=amount_created
            )
            
            if(txType == "ExportTx"):
                # env inputs 
                for env_inputs in tx.get('evmInputs', []):
                    
                    asset =  env_inputs['asset']
                    
                    emit_utxo = AvalancheUTXO(
                            txHash = txHash,
                            txType = txType,
                            blockHash = blockHash,
                            addresses = env_inputs['fromAddress'],
                            utxoId= None,
                            assetId = asset.get('assetId'),
                            asset_name = asset.get('name', ''),
                            symbol = asset.get('symbol', ''),
                            denomination = asset.get('denomination',0),
                            asset_type = asset.get('type',''),
                            amount = asset.get('amount',0)
                        )
                    
                    input_env.append(emit_utxo.__dict__)

                # emmitted UTXOs
                for e_utxo in tx.get('emittedUtxos', []):
                    
                    asset =  e_utxo['asset']
                    
                    emit_utxo = AvalancheUTXO(
                            txHash = txHash,
                            txType = txType,
                            blockHash = blockHash,
                            addresses = e_utxo['addresses'],
                            utxoId= e_utxo['utxoId'],
                            assetId = asset.get('assetId'),
                            asset_name = asset.get('name', ''),
                            symbol = asset.get('symbol', ''),
                            denomination = asset.get('denomination',0),
                            asset_type = asset.get('type',''),
                            amount = asset.get('amount',0)
                        )
                    
                    emitted_utxos.append(emit_utxo.__dict__)
                    
            elif(txType == "ImportTx"):
                
                for env_inputs in tx.get('evmOutputs', []):
                    asset =  env_inputs['asset']
                    
                    emit_utxo = AvalancheUTXO(
                            txHash = txHash,
                            txType = txType,
                            blockHash = blockHash,
                            addresses = env_inputs['toAddress'],
                            utxoId= None,
                            assetId = asset.get('assetId'),
                            asset_name = asset.get('name', ''),
                            symbol = asset.get('symbol', ''),
                            denomination = asset.get('denomination',0),
                            asset_type = asset.get('type',''),
                            amount = asset.get('amount',0)
                        )
                    
                    output_env.append(emit_utxo.__dict__)

                # emmitted UTXOs
                for e_utxo in tx.get('consumedUtxos', []):
                    
                    
                    asset =  e_utxo['asset']
                    
                    emit_utxo = AvalancheUTXO(
                            txHash = txHash,
                            txType = txType,
                            blockHash = blockHash,
                            addresses = e_utxo['addresses'],
                            utxoId= e_utxo['utxoId'],
                            assetId = asset.get('assetId'),
                            asset_name = asset.get('name', ''),
                            symbol = asset.get('symbol', ''),
                            denomination = asset.get('denomination',0),
                            asset_type = asset.get('type',''),
                            amount = asset.get('amount',0)
                        )
                    consumed_utxos.append(emit_utxo.__dict__)
            page_token = res_data.get('nextPageToken')
            
            data.append(avalanche_tx.__dict__)
    
    return pd.DataFrame(data), pd.DataFrame(input_env), pd.DataFrame(output_env), pd.DataFrame(consumed_utxos), pd.DataFrame(emitted_utxos)



def extract_p_chain_data(last_timestamp):
    page_token = None
    params = {"pageSize": 100}
    url = "https://glacier-api.avax.network/v1/networks/mainnet/blockchains/p-chain/transactions"
    data = []
    emitted_utxos= []
    consumed_utxos = []
    
    run = True

    while run:
        if page_token:
            params["pageToken"] = page_token
        
        res_data = fetch_transactions(url, params)
        transactions = res_data.get('transactions', [])

        for tx in transactions:
            timestamp = int(tx.get("blockTimestamp"))
            if timestamp <= last_timestamp:
                run = False
                break
                
            amountStaked = calculate_p_transaction_value(tx.get("amountStaked", []))
            amountBurned = calculate_p_transaction_value(tx.get("amountBurned", []))

            txHash=tx.get("txHash")
            blockHash=tx.get("blockHash")
            txType=tx.get("txType")
            
            p_tx = Avalanche_P_Model(
                txHash=tx.get("txHash"),
                txType=tx.get("txType"),
                blockTimestamp=timestamp,
                blockNumber=tx.get("blockNumber"),
                blockHash=tx.get("blockHash"),
                sourceChain = tx.get("sourceChain",''),
                destinationChain =  tx.get("destinationChain",''),
                memo=tx.get("memo"),
                rewardAddresses = tx.get("rewardAddresses",''),
                estimatedReward = tx.get("estimatedReward",''),
                startTimestamp = tx.get("startTimestamp",''),
                endTimestamp = tx.get("endTimestamp",''),
                delegationFeePercent = tx.get("delegationFeePercent",''),
                nodeId=tx.get("nodeId",''),
                subnetId=tx.get("subnetId",''),
                value = tx.get("value",''),
                amountStaked=amountStaked,
                amountBurned=amountBurned,
            )
            
            # emmitted UTXOs
            for e_utxo in tx.get('emittedUtxos', []):
                
                
                asset =  e_utxo['asset']
                
                emit_utxo = AvalancheUTXO(
                        txHash = txHash,
                        txType=txType,
                        blockHash = blockHash,
                        addresses = e_utxo['addresses'],
                        utxoId= e_utxo['utxoId'],
                        assetId = asset.get('assetId'),
                        asset_name = asset.get('name', ''),
                        symbol = asset.get('symbol', ''),
                        denomination = asset.get('denomination',0),
                        asset_type = asset.get('type',''),
                        amount = asset.get('amount',0)
                    )
                
                emitted_utxos.append(emit_utxo.__dict__)
            
            # consumed UTXOs
            for c_utxo in tx.get('consumedUtxos', []):
                
                asset =  c_utxo['asset']
                
                commit_utxo = AvalancheUTXO(
                        txHash = txHash,
                        txType = txType,
                        blockHash = blockHash,
                        addresses = c_utxo['addresses'],
                        utxoId= c_utxo['utxoId'],
                        assetId = asset.get('assetId'),
                        asset_name = asset.get('name', ''),
                        symbol = asset.get('symbol', ''),
                        denomination = asset.get('denomination',0),
                        asset_type = asset.get('type',''),
                        amount = asset.get('amount',0)
                    )
                consumed_utxos.append(commit_utxo.__dict__)
            
            data.append(p_tx.__dict__)
        page_token = res_data.get('nextPageToken')
    
    return pd.DataFrame(data), pd.DataFrame(emitted_utxos), pd.DataFrame(consumed_utxos)


def calculate_p_transaction_value(amounts):
    total_value = sum(int(asset['amount']) for asset in amounts) / 10**9  # Convert to AVAX
    return total_value

#---------------------------------------------------------

def extract_avalanche_data(last_x_time, last_c_time,last_p_time):
    x_chain_data = extract_x_chain_data(last_x_time)
    c_chain_data = extract_c_chain_data(last_c_time)
    p_chain_data = extract_p_chain_data(last_p_time)

    return x_chain_data, c_chain_data, p_chain_data
    # return x_chain_data

class EVMInput:
    def __init__(self, asset, fromAddress, credentials):
        self.asset = Asset(**asset)
        self.fromAddress = fromAddress
        self.credentials = credentials

class Asset:
    def __init__(self, assetId, name, symbol, denomination, type, amount):
        self.assetId = assetId
        self.name = name
        self.symbol = symbol
        self.denomination = denomination
        self.type = type
        self.amount = amount
        
if __name__ == "__main__":
    extract_p_chain_data(1705276800)
    extract_c_chain_data(1705276800)
    extract_x_chain_data(1705276800)