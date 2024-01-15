# app.py
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
from scripts.workflow_manager import WorkflowManager

# from 'scripts.workflow_manager' import WorkflowManager
from scripts.workflow_manager import WorkflowManager

app = Flask(__name__)

def run_workflow_manager():
    manager = WorkflowManager()
    manager.run()  # Assuming `run` is the method to start the workflow

scheduler = BackgroundScheduler()
scheduler.add_job(func=run_workflow_manager, trigger="interval", seconds=3600)
scheduler.start()

if __name__ == "__main__":
    app.run()