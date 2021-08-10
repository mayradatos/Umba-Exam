from airflow.decorators import task
from airflow.hooks.S3_hook import S3Hook

import pandas as pd
import numpy as np


@task
def VisualAnalysis(dataPath):
    """
    #### Visual Analysis
    Renders multiple graphs that help explore the data.
    """
    # ti = kwargs["ti"]
    # dataPath = ti.xcom_pull(task_ids="Load", key="dataPath")
    print(dataPath)


@task
def SimpleAnalysis(dataPath, **kwargs):
    """
    #### Simple Analysis
    Prints a simple summary of the data.
    """
    import io

    buffer = io.StringIO()
    s3 = S3Hook(aws_conn_id="custom_s3")
    downloadPath = s3.download_file(key=dataPath, bucket_name="dag-umba")
    df_credit = pd.read_csv(downloadPath, index_col=0)
    df_credit.info(buf=buffer)
    with open("SimpleAnalysis.txt", "w", encoding="utf-8") as f:
        f.write(
            "\n Searching for Missings,type of data and also known the shape of data \n"
        )
        f.write(buffer.getvalue() + "\n")
        f.write("\n Looking unique values \n")
        f.write(df_credit.nunique().to_string() + "\n")
        f.write("\n Purpose \n")
        f.write(np.array_str(df_credit.Purpose.unique()))
        f.write("\n Sex \n")
        f.write(np.array_str(df_credit.Sex.unique()))
        f.write("\n Housing \n")
        f.write(np.array_str(df_credit.Housing.unique()))
        f.write("\n Saving accounts \n")
        f.write(np.array_str(df_credit["Saving accounts"].unique()))
        f.write("\n Risk \n")
        f.write(np.array_str(df_credit["Risk"].unique()))
        f.write("\n Checking account \n")
        f.write(np.array_str(df_credit["Checking account"].unique()))
        f.write("\n Aget_cat \n")
        f.write("[" + ",".join(["Student", "Young", "Adult", "Senior"]) + "]")
        f.write("\n Looking the data \n")
        f.write(df_credit.head().T.to_string())
        f.write(
            "\n \n \n Crosstab session and anothers to explore our data by another metrics a little deep \n"
        )
        f.write(
            "\n Crosstab to define the type of job a person have depending on his sex \n"
        )
        f.write(pd.crosstab(df_credit.Sex, df_credit.Job).to_string() + "\n")
        f.write(
            "\n Crosstab to define the checking account a person have depending on his sex \n"
        )
        f.write(
            pd.crosstab(df_credit["Checking account"], df_credit.Sex).to_string() + "\n"
        )
        f.write(
            "\n Crosstab to define the purpose a person have depending on his sex \n"
        )
        f.write(pd.crosstab(df_credit["Purpose"], df_credit["Sex"]).to_string() + "\n")
    # Upload to S3
    ds = kwargs["execution_date"].strftime("%Y-%m-%d-%H-%M-%S")
    key = ds + "/SimpleAnalysis.txt"
    s3.load_file(
        bucket_name="dag-umba",
        key=key,
        filename="SimpleAnalysis.txt",
        replace=True,
    )
    print("SUCCESS: " + key)


@task
def CorrelationGrid(dataPath, **kwargs):
    """
    #### Correlation Grid
    Renders a heatmap of the correlation between all numeric columns in the data.
    """
    import matplotlib.pyplot as plt  # to plot some parameters in seaborn
    import seaborn as sns  # Graph library that use matplot in background

    s3 = S3Hook(aws_conn_id="custom_s3")
    downloadPath = s3.download_file(key=dataPath, bucket_name="dag-umba")
    df_credit = pd.read_csv(downloadPath, index_col=0)
    plt.figure(figsize=(14, 12))
    sns.heatmap(
        df_credit.astype(float).corr(),
        linewidths=0.1,
        vmax=1.0,
        square=True,
        linecolor="white",
        annot=True,
    )
    # plt.show()
    plt.savefig("correlation_grid.png")
    # Upload to S3
    ds = kwargs["execution_date"].strftime("%Y-%m-%d-%H-%M-%S")
    key = ds + "/correlation_grid.png"
    s3 = S3Hook(aws_conn_id="custom_s3")
    s3.load_file(
        bucket_name="dag-umba",
        key=key,
        filename="correlation_grid.png",
        replace=True,
    )
    print("SUCCESS: " + key)


def branch(**kwargs):
    ti = kwargs["ti"]
    dataPath = ti.xcom_pull(task_ids="PrepareData")
    print("RECIBI: " + dataPath)
    return "Model_1"
