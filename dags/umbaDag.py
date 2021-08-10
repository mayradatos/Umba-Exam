from textwrap import dedent

# from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.hooks.S3_hook import S3Hook
from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

from pprint import pprint

# Import Custom Tasks
from tasks import umbaTasks, dataLoaders

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
    schedule_interval=None,
    start_date=days_ago(2),
    # tags=["example"],
)
def UmbaExam():
    # with DAG(
    #     "Umba-Exam",
    #     default_args=default_args,
    #     description="Admission exam for Big Data Engineer position at Umba",
    #     schedule_interval=timedelta(days=1),
    #     start_date=days_ago(2),
    #     tags=["example"],
    #     # template_searchpath="/usr/local/airflow/include",  # include path to look for external files
    # ) as dag:
    """
    ### UMBA Exam
    This is a simple exam for Big Data Engineer position at Umba.
    """

    # def branch(dataPath):
    #     # Evaluate models performance
    #     print("Hello World")
    branch = BranchPythonOperator(
        task_id="SelectModel", python_callable=umbaTasks.branch, provide_context=True
    )
    branch.doc_md = dedent(
        """
            #### Branching Task
            Evaluate models performance and determine which model to use.
            """
    )

    a = []
    for i in range(0, 5):
        a.append(DummyOperator(task_id="Model_" + str(i)))
        branch >> a[i]

    dP = dataLoaders.DataLoad()
    umbaTasks.SimpleAnalysis(dP)
    umbaTasks.VisualAnalysis(dP)

    preparedDataPath = dataLoaders.PrepareData()
    dP >> preparedDataPath >> branch
    umbaTasks.CorrelationGrid(preparedDataPath)

    # @task
    # def GetFiles():
    #     s3 = S3Hook(aws_conn_id="custom_s3")
    #     # files = s3.list_keys(bucket_name="test-bucket")
    #     # print("BUCKET:  {}".format(files))
    #     # https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/hooks/s3/index.html#airflow.providers.amazon.aws.hooks.s3.S3Hook.download_file
    #     path = s3.download_file(bucket_name="test-bucket", key="german_credit_data.csv")
    #     print(path)
    #     # Importing the data
    #     df_credit = pd.read_csv(path, index_col=0)
    #     print(df_credit.info())
    #     # it's a library that we work with plotly
    #     import plotly.offline as py

    #     # py.init_notebook_mode(
    #     #     connected=True
    #     # )  # this code, allow us to work with offline plotly version

    #     import plotly.graph_objs as go  # it's like "plt" of matplot
    #     import plotly.tools as tls  # It's useful to we get some tools of plotly
    #     import warnings  # This library will be used to ignore some warnings
    #     from collections import Counter  # To do counter of some features

    #     trace0 = go.Bar(
    #         x=df_credit[df_credit["Risk"] == "good"]["Risk"]
    #         .value_counts()
    #         .index.values,
    #         y=df_credit[df_credit["Risk"] == "good"]["Risk"].value_counts().values,
    #         name="Good credit",
    #     )

    #     trace1 = go.Bar(
    #         x=df_credit[df_credit["Risk"] == "bad"]["Risk"].value_counts().index.values,
    #         y=df_credit[df_credit["Risk"] == "bad"]["Risk"].value_counts().values,
    #         name="Bad credit",
    #     )

    #     data = [trace0, trace1]

    #     layout = go.Layout()

    #     layout = go.Layout(
    #         yaxis=dict(title="Count"),
    #         xaxis=dict(title="Risk Variable"),
    #         title="Target variable distribution",
    #     )

    #     fig = go.Figure(data=data, layout=layout)
    #     # fig.write_html(
    #     #     "Risk Variable Distribution.html",
    #     #     full_html=False,
    #     #     include_plotlyjs="cdn",
    #     # )
    #     print("Values describe: ")
    #     print(pd.crosstab(df_credit.Purpose, df_credit.Risk))

    #     plt.figure(figsize=(14, 12))

    #     plt.subplot(221)
    #     g = sns.countplot(x="Purpose", data=df_credit, palette="hls", hue="Risk")
    #     g.set_xticklabels(g.get_xticklabels(), rotation=45)
    #     g.set_xlabel("", fontsize=12)
    #     g.set_ylabel("Count", fontsize=12)
    #     g.set_title("Purposes Count", fontsize=20)

    #     plt.subplot(222)
    #     g1 = sns.violinplot(
    #         x="Purpose", y="Age", data=df_credit, palette="hls", hue="Risk", split=True
    #     )
    #     g1.set_xticklabels(g1.get_xticklabels(), rotation=45)
    #     g1.set_xlabel("", fontsize=12)
    #     g1.set_ylabel("Count", fontsize=12)
    #     g1.set_title("Purposes by Age", fontsize=20)

    #     plt.subplot(212)
    #     g2 = sns.boxplot(
    #         x="Purpose", y="Credit amount", data=df_credit, palette="hls", hue="Risk"
    #     )
    #     g2.set_xlabel("Purposes", fontsize=12)
    #     g2.set_ylabel("Credit Amount", fontsize=12)
    #     g2.set_title("Credit Amount distribuition by Purposes", fontsize=20)

    #     plt.subplots_adjust(hspace=0.6, top=0.8)

    #     plt.show()

    #     with open("p_graph.html", "a") as f:
    #         f.write(fig.to_html(full_html=False, include_plotlyjs="cdn"))
    #         f.write(fig2.to_html(full_html=False, include_plotlyjs="cdn"))

    #     # Upload to S3
    #     s3.load_file(
    #         bucket_name="test-bucket",
    #         key="Risk Variable Distribution.html",
    #         filename="p_graph.html",
    #     )

    # GetFiles()
    # "Airflow sends out Tasks to run on Workers as space becomes available, so there’s no guarantee all the tasks in your DAG will run on the same worker or the same machine." https://airflow.apache.org/docs/apache-airflow/stable/concepts/overview.html#control-flow

    # t1 = PythonVirtualenvOperator(
    #     task_id="download",
    #     python_callable=GetFiles,
    #     system_site_packages=True,
    #     requirements=["seaborn"],
    #     # provide_context=False,
    #     # op_kwargs={
    #     #     execution_date_str: "{{ execution_date }}",
    #     # },
    #     # dag=dag,
    # )

    # check_for_file = PythonOperator(
    #     task_id="download",
    #     # provide_context=True,
    #     python_callable=GetFiles,
    #     # dag=dag,
    # )

    # s3_file = S3ListOperator(
    #     task_id="list_3s_files",
    #     bucket="test-bucket",
    #     # prefix="/",
    #     # delimiter="/",
    #     aws_conn_id="custom_s3",
    # )
    # t1, t2 and t3 are examples of tasks created by instantiating operators
    # t1 = BashOperator(
    #     task_id="print_date",
    #     bash_command="TZ='America/Mexico_City' date",
    # )

    # t2 = BashOperator(
    #     task_id="sleep",
    #     depends_on_past=False,
    #     bash_command="sleep 5",
    #     retries=3,
    # )
    # GetFiles.doc_md = dedent(
    #     """\
    # #### Task Documentation
    # You can document your task using the attributes `doc_md` (markdown),
    # `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    # rendered in the UI's Task Instance Details page.
    # ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)

    # """

    # templated_command = dedent(
    #     """
    # {% for i in range(5) %}
    #     echo "{{ ds }}"
    #     echo "{{ macros.ds_add(ds, 7)}}"
    #     echo "{{ params.my_param }}"
    # {% endfor %}
    # """
    # )

    # t3 = BashOperator(
    #     task_id="templated",
    #     depends_on_past=False,
    #     bash_command=templated_command,
    #     params={"my_param": "Parameter I passed in"},
    # )

    # t1 >> [t2, t3]


UmbaExam = UmbaExam()
