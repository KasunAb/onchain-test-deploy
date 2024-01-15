# workflow_manager.py
import sys
import os
import datetime
import logging
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.blockchain.avalanche.avalanche_data_extraction import extract_avalanche_data
from src.services.data_storage_service import store_data, get_last_transaction_data, set_last_transaction_data
from src.services.metrics_computation_service import compute_transaction_count,compute_average_transactions_per_block,compute_total_staked_amount,compute_total_burned_amount, compute_average_transaction_value, compute_large_transaction_monitoring, compute_cross_chain_whale_activity
# from data_processing import process_data

import os
from dotenv import load_dotenv

load_dotenv()

class WorkflowManager:
    def __init__(self):
        # Initialize any required variables, connections, etc.
        self.logger = logging.getLogger(__name__)
        
        # TO DO : add following links to env
        self.file_path = "file_store"
        self.db_connection_string = os.environ.get("DATABASE_CONNECTION")

    def run_avalanche_data_workflow(self):
        """
        Orchestrates the workflow for extracting, processing, and storing Avalanche blockchain data.
        """
        
        # TO DO : get last day which data stored
        try:
            # Step 1: Extract data
            self.logger.info("Extracting Avalanche data...")
            # last_time_stamp_x = get_last_transaction_data(os.environ.get("DATABASE_CONNECTION"), "AVALANCHE_X_CHAIN")
            # last_time_stamp_c = get_last_transaction_data(os.environ.get("DATABASE_CONNECTION"), "AVALANCHE_C_CHAIN")
            # last_time_stamp_p = get_last_transaction_data(os.environ.get("DATABASE_CONNECTION"), "AVALANCHE_P_CHAIN")
            
            last_time_stamp_x = 1705084200
            last_time_stamp_c = 1705084200
            last_time_stamp_c = 1705084200
            
            avalanche_X_data,avalanche_C_data, avalanche_P_data = extract_avalanche_data(last_time_stamp_x, last_time_stamp_c, last_time_stamp_c)

            if avalanche_X_data.empty or avalanche_C_data.empty or avalanche_P_data.empty:
                self.logger.info("No data extracted.")
                return

            # Step 2: Process data
            self.logger.info("Processing data...")
            max_timstamp_x = avalanche_X_data['timestamp'].max()
            max_timstamp_c = avalanche_C_data['timestamp'].max()
            max_timstamp_p = avalanche_P_data['timestamp'].max()
            
            # Step 3: Store data
            self.logger.info("Storing data...")
            
            date = datetime.datetime.fromtimestamp(max_timstamp_x) 
            date_str = date.strftime("%Y-%m-%d")
            
            if not os.path.exists(self.file_path):
                # Create the directory
                os.makedirs(self.file_path)
                # Get the directory of the current script
                
            script_directory = os.path.dirname(os.path.abspath(__file__))

            # Get the parent directory of the script directory
            parent_directory = os.path.dirname(script_directory)

            # Set the file_path to the file_store directory parallel to the src directory
            file_path = os.path.join(parent_directory, 'file_store')
            
            store_data(avalanche_X_data, file_path+f"/{date_str}_x_file.tsv.gz", 'x_avalanche_data', self.db_connection_string)
            store_data(avalanche_C_data, file_path+f"/{date_str}_c_file.tsv.gz", 'c_avalanche_data', self.db_connection_string)
            store_data(avalanche_C_data, file_path+f"/{date_str}_p_file.tsv.gz", 'p_avalanche_data', self.db_connection_string)
            self.logger.info("Stored data !")
            
            set_last_transaction_data(os.environ.get("DATABASE_CONNECTION"), "AVALANCHE_X_CHAIN", max_timstamp_x)
            set_last_transaction_data(os.environ.get("DATABASE_CONNECTION"), "AVALANCHE_C_CHAIN", max_timstamp_c)
            set_last_transaction_data(os.environ.get("DATABASE_CONNECTION"), "AVALANCHE_P_CHAIN", max_timstamp_p)
            
            # Step 3: metrics
            # TO DO : Metric computations
            
            self.logger.info("Computing metrics...")
            
            compute_transaction_count(avalanche_X_data, date_str, "X_CHAIN", os.environ.get("DATABASE_CONNECTION"))
            self.logger.info("Computed transaction count !")
            compute_average_transactions_per_block(avalanche_X_data, date_str, "X_CHAIN", os.environ.get("DATABASE_CONNECTION"))
            self.logger.info("Computed average transactions per block !")
            compute_total_staked_amount(avalanche_P_data, date_str, "X_CHAIN", os.environ.get("DATABASE_CONNECTION"))
            self.logger.info("Computed total staked amount !")
            compute_total_burned_amount(avalanche_P_data, date_str, "X_CHAIN", os.environ.get("DATABASE_CONNECTION"))
            self.logger.info("Computed total staked amount !")
            compute_average_transaction_value(avalanche_C_data, date_str, "X_CHAIN", os.environ.get("DATABASE_CONNECTION"))
            self.logger.info("Computed average transaction value !")
            compute_large_transaction_monitoring(avalanche_C_data, date_str, "X_CHAIN", os.environ.get("DATABASE_CONNECTION"))
            self.logger.info("Computed large transaction monitoring !")
            compute_cross_chain_whale_activity(avalanche_C_data, date_str, "X_CHAIN", os.environ.get("DATABASE_CONNECTION"))
            self.logger.info("Computed cross chain whale activity !")
            
            self.logger.info("Workflow completed successfully.")
            
        except Exception as e:
            self.logger.error(f"An error occurred during the workflow: {e}")
            # Handle or raise the exception as per your error handling policy

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)  # Set up basic logging configuration
    manager = WorkflowManager()
    manager.run_avalanche_data_workflow()
    
