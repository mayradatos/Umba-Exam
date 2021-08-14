from textwrap import dedent

# from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.hooks.S3_hook import S3Hook
from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

from pprint import pprint

# Import Custom Tasks
from umbaProject import tasks, dataLoaders, models, graphs
from umbaProject.models import *

# Common Libaries
import pandas as pd  # To work with dataset

# import numpy as np  # Math library
# import seaborn as sns  # Graph library that use matplot in background
# import matplotlib.pyplot as plt  # to plot some parameters in seaborn

default_args = {
    "owner": "Mayra Patricia",
    "email": ["mayra.patricia@hotmail.com"],
    # "email_on_failure": False,
    # "email_on_retry": False,
    # "retries": 1,
    # "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}


@dag(
    default_args=default_args,
    start_date=days_ago(2),
    tags=["Evaluation"],
)
def UmbaExam():
    """
    ### UMBA Exam
    This is an exam for Big Data Engineer position at Umba.
    """

    # Analysis Nodes depend on Data Loader
    data = dataLoaders.DataLoad()
    tasks.SimpleAnalysis(data)
    graphs.VisualAnalysis(data)

    # Create branching to select best model
    modelSelection = BranchPythonOperator(
        task_id="SelectModel", python_callable=tasks.branch
    )
    modelSelection.doc_md = dedent(
        """
        #### Branching Task
        Evaluate models performance and determine which model to use.
        """
    )

    # Data preparation depends on Data Loader
    # And model selection depends on data preparation
    cleanData = dataLoaders.PrepareData()
    data >> cleanData >> modelSelection

    # Correlation grid depends only on data preparation
    tasks.CorrelationGrid(cleanData["dataKey"])

    # Create ML model tasks
    modelDict = {}

    # Add implemented models to dictionary
    modelDict["NB"] = Model_NB()
    modelDict["RF"] = Model_RF()
    modelDict["XGB"] = Model_XGB()

    # Create Dummy Model task for all other models
    def dummyModel(**kwargs):
        print("Sample Model: Does nothing")
        return {
            "model": "Sample Model",
            "precision": 0.5,
            "recall": 0.5,
        }

    # Create Final node which depends on any one of the models not failing
    @task(trigger_rule="none_failed")
    def WriteOutput(**kwargs):
        ti = kwargs["ti"]
        pipelineResult = ti.xcom_pull(task_ids="Custom_Pipeline")
        selectedModel = ti.xcom_pull(task_ids="SelectModel")
        results = ti.xcom_pull(task_ids=selectedModel)
        print("Received:" + str(results))
        print("Pipeline Result:" + str(pipelineResult))

    # Add models both real and dummy to dependency graph
    finalNode = WriteOutput()
    #  TODO: Cambiar por lista dinameica de nombres
    for modelName in ["LR", "LDA", "KNN", "CART", "NB", "RF", "SVM", "XGB"]:
        if modelName not in modelDict:
            modelDict[modelName] = PythonOperator(
                task_id="Model_" + modelName, python_callable=dummyModel
            )
        modelSelection >> modelDict[modelName] >> finalNode

    # Custom model pipeline will be executed concurrent to model selection
    cleanData >> Custom_Pipeline(cleanData["dataKey"]) >> finalNode


UmbaExam = UmbaExam()
