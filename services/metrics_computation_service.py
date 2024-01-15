import pandas as pd
import psycopg2
import numpy as np
from sqlalchemy import create_engine

#Daily Transaction Count - X, P, C
def compute_transaction_count(dataframe, date, chain, db_connection_string, table_name = 'daily_transaction_count'):

    # Connect to PostgreSQL Server
    conn = psycopg2.connect(db_connection_string)
    conn.autocommit = True
    cursor = conn.cursor()
    
    # Calculate transaction count
    transaction_count = len(dataframe)

    # Define the table name
    
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            date VARCHAR(100),
            chain_name VARCHAR(255),
            count INTEGER
        );
        INSERT INTO {table_name} (date, chain_name, count) VALUES (%s, %s, %s)""",
        (date, chain, transaction_count))

    cursor.close()
    conn.close()
    return transaction_count

# Average Transactions Per Block - X
def compute_average_transactions_per_block(dataframe, date, chain, db_connection_string, table_name='average_transactions_per_block'):
    conn = psycopg2.connect(db_connection_string)
    conn.autocommit = True
    cursor = conn.cursor()

    # Group by blockHash and calculate average transactions per block
    avg_transactions_per_block = dataframe.groupby('blockHash').size().mean()

    # Create table and insert data
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            date VARCHAR(100),
            chain_name VARCHAR(255),
            avg_transactions_per_block FLOAT
        );
        INSERT INTO {table_name} (date, chain_name, avg_transactions_per_block) VALUES (%s, %s, %s)""",
                   (date, chain, avg_transactions_per_block))

    cursor.close()
    conn.close()
    return avg_transactions_per_block

# Total Staked Amount - P
def compute_total_staked_amount(dataframe, date, chain, db_connection_string, table_name='total_staked_amount'):
    conn = psycopg2.connect(db_connection_string)
    conn.autocommit = True
    cursor = conn.cursor()

    # Calculate total staked amount
    total_staked_amount = dataframe['amountStaked'].sum()

    # Create table and insert data
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            date VARCHAR(100),
            chain_name VARCHAR(255),
            total_staked_amount FLOAT
        );
        INSERT INTO {table_name} (date, chain_name, total_staked_amount) VALUES (%s, %s, %s)""",
                   (date, chain, total_staked_amount))

    cursor.close()
    conn.close()
    return total_staked_amount

# Total Burned Amount - P
def compute_total_burned_amount(dataframe, date, chain, db_connection_string, table_name='total_burned_amount'):
    conn = psycopg2.connect(db_connection_string)
    conn.autocommit = True
    cursor = conn.cursor()

    # Calculate total burned amount
    total_burned_amount = dataframe['amountBurned'].sum()

    # Create table and insert data
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            date VARCHAR(100),
            chain_name VARCHAR(255),
            total_burned_amount FLOAT
        );
        INSERT INTO {table_name} (date, chain_name, total_burned_amount) VALUES (%s, %s, %s)""",
                   (date, chain, total_burned_amount))

    cursor.close()
    conn.close()
    return total_burned_amount

# Average Transaction Value - C
def compute_average_transaction_value(dataframe, date, chain, db_connection_string, table_name='average_transaction_value'):
    conn = psycopg2.connect(db_connection_string)
    conn.autocommit = True
    cursor = conn.cursor()

    # Calculate average transaction value
    average_transaction_value = (dataframe['total_input_value'] + dataframe['total_output_value']).mean()

    # Create table and insert data
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            date VARCHAR(100),
            chain_name VARCHAR(255),
            average_transaction_value FLOAT
        );
        INSERT INTO {table_name} (date, chain_name, average_transaction_value) VALUES (%s, %s, %s)""",
                   (date, chain, average_transaction_value))

    cursor.close()
    conn.close()
    return average_transaction_value

# Large Transaction Monitoring (Whale Watching) - C
def compute_large_transaction_monitoring(dataframe, date, chain, db_connection_string, table_name='large_transaction_monitoring', large_transaction_threshold=1000000):
    conn = psycopg2.connect(db_connection_string)
    conn.autocommit = True
    cursor = conn.cursor()

    # Filter large transactions
    large_transactions = dataframe[dataframe['total_output_value'] > large_transaction_threshold]

    # Count of large transactions
    large_transaction_count = len(large_transactions)

    # Create table and insert data
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            date VARCHAR(100),
            chain_name VARCHAR(255),
            large_transaction_count INTEGER
        );
        INSERT INTO {table_name} (date, chain_name, large_transaction_count) VALUES (%s, %s, %s)""",
                   (date, chain, large_transaction_count))

    cursor.close()
    conn.close()
    return large_transaction_count

# Cross-Chain Whale Activity (Whale Watching) - C
def compute_cross_chain_whale_activity(dataframe, date, chain, db_connection_string, table_name='cross_chain_whale_activity', large_transaction_threshold=1000000):
    conn = psycopg2.connect(db_connection_string)
    conn.autocommit = True
    cursor = conn.cursor()

    # Filter cross-chain large transactions
    cross_chain_large_transactions = dataframe[(dataframe['total_output_value'] > large_transaction_threshold) & (dataframe['sourceChain'] != dataframe['destinationChain'])]

    # Count of cross-chain large transactions
    cross_chain_large_transaction_count = len(cross_chain_large_transactions)

    # Create table and insert data
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            date VARCHAR(100),
            chain_name VARCHAR(255),
            cross_chain_large_transaction_count INTEGER
        );
        INSERT INTO {table_name} (date, chain_name, cross_chain_large_transaction_count) VALUES (%s, %s, %s)""",
                   (date, chain, cross_chain_large_transaction_count))

    cursor.close()
    conn.close()
    return cross_chain_large_transaction_count

def calculate_average_utxo_value(transactions):
    total_utxo_value = 0
    total_utxo_count = 0

    for tx in transactions:
        # Include both consumed and emitted UTXOs
        all_utxos = tx.consumedUtxos + tx.emittedUtxos
        for utxo in all_utxos:
            total_utxo_value += utxo.amount
            total_utxo_count += 1

    # Calculate the average
    return total_utxo_value / total_utxo_count if total_utxo_count > 0 else 0

def cross_chain_whale_activity(transactions, value_threshold, other_chain_transactions):
    cross_chain_whales = []
    for tx in transactions:
        if tx.sourceChain != tx.destinationChain and tx.value >= value_threshold:
            cross_chain_whales.append(tx)
    # Check for corresponding transactions in other_chain_transactions
    # ...
    return cross_chain_whales

def monitor_whale_addresses(transactions, balance_threshold, historical_balances):
    whale_addresses = set()
    for tx in transactions:
        for utxo in tx.consumedUtxos + tx.emittedUtxos:
            address = utxo.address
            new_balance = historical_balances.get(address, 0) + utxo.amount
            if new_balance >= balance_threshold:
                whale_addresses.add(address)
    return whale_addresses

def identify_whale_transactions(transactions, value_threshold):
    whale_transactions = []
    for tx in transactions:
        if tx.value >= value_threshold:
            whale_transactions.append(tx)
    return whale_transactions

def calculate_tps(dataframe):
    total_seconds = (dataframe['blockTimestamp'].max() - dataframe['blockTimestamp'].min())
    if total_seconds > 0:
        return len(dataframe) / total_seconds
    else:
        return 0

def compute_transactions_per_day(dataframe):
    return len(dataframe)

def calculate_total_addresses(dataframe):
    addresses = set()
    for _, row in dataframe.iterrows():
        for utxo in row['emittedUtxos'] + row['consumedUtxos']:
            addresses.update(utxo.addresses)
    return len(addresses)

def calculate_total_addresses(dataframe):
    addresses = set()
    for _, row in dataframe.iterrows():
        for utxo in row['emittedUtxos'] + row['consumedUtxos']:
            addresses.update(utxo.addresses)
    return len(addresses)

def calculate_active_senders(dataframe):
    senders = set()
    for _, row in dataframe.iterrows():
        for evmInput in row.get('evmInputs', []):
            senders.add(evmInput.fromAddress)
    return len(senders)

def calculate_average_utxo_value(dataframe):
    total_utxo_value = 0
    total_utxos = 0
    for _, row in dataframe.iterrows():
        for utxo in row['emittedUtxos'] + row['consumedUtxos']:
            total_utxo_value += int(utxo.asset.amount)
            total_utxos += 1
    return total_utxo_value / total_utxos if total_utxos > 0 else 0



