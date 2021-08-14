from textwrap import dedent

from airflow.utils.dates import days_ago
from airflow.decorators import dag
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

# Import Custom Tasks
from umbaProject import tasks, dataLoaders, models, graphs

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
    modelDict["NB"] = models.Model_NB()
    modelDict["RF"] = models.Model_RF()
    modelDict["XGB"] = models.Model_XGB()

    # Create Dummy Model task for all other models
    def dummyModel(**kwargs):
        print("Sample Model: Does nothing")
        return {
            "model": "Sample Model",
            "precision": 0.5,
            "recall": 0.5,
        }

    # Add models both real and dummy to dependency graph
    finalNode = tasks.WriteOutput()
    for modelName in ["LR", "LDA", "KNN", "CART", "NB", "RF", "SVM", "XGB"]:
        if modelName not in modelDict:
            modelDict[modelName] = PythonOperator(
                task_id="Model_" + modelName, python_callable=dummyModel
            )
        modelSelection >> modelDict[modelName] >> finalNode

    # Custom model pipeline will be executed concurrent to model selection
    cleanData >> models.Custom_Pipeline() >> finalNode


UmbaExam = UmbaExam()
