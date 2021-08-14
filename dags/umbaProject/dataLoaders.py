from airflow.decorators import task
from airflow.hooks.S3_hook import S3Hook

import pandas as pd
import numpy as np


@task()
def DataLoad(**kwargs):
    """
    #### Load Node
    A simple task which downloads relevant data from S3 and makes it available on the common `include` folder.
    """
    s3 = S3Hook(aws_conn_id="custom_s3")
    downloadPath = s3.download_file(
        key="data/german_credit_data.csv",
        bucket_name="dag-umba",
    )
    df_credit = pd.read_csv(downloadPath, index_col=0)
    interval = (18, 25, 35, 60, 120)
    cats = ["Student", "Young", "Adult", "Senior"]
    df_credit["Age_cat"] = pd.cut(df_credit.Age, interval, labels=cats)
    df_credit.to_csv(r"german_credit_data_LDA.csv", index=False)
    # Upload to S3
    uploadKey = (
        kwargs["execution_date"].strftime("%Y-%m-%d-%H-%M-%S")
        + "/german_credit_data_LDA.csv"
    )
    s3.load_file(
        bucket_name="dag-umba",
        key=uploadKey,
        filename="german_credit_data_LDA.csv",
        replace=True,
    )
    return uploadKey


@task(multiple_outputs=True)
def PrepareData(**kwargs):
    """
    #### Prepare Data
    A task which prepares the data for all ML models.
    """
    from sklearn.model_selection import train_test_split

    s3 = S3Hook(aws_conn_id="custom_s3")
    ds = kwargs["execution_date"].strftime("%Y-%m-%d-%H-%M-%S")
    downloadPath = s3.download_file(
        key=ds + "/german_credit_data_LDA.csv",
        bucket_name="dag-umba",
    )
    df_credit = pd.read_csv(downloadPath, index_col=0)
    df_credit["Saving accounts"] = df_credit["Saving accounts"].fillna("no_inf")
    df_credit["Checking account"] = df_credit["Checking account"].fillna("no_inf")

    df_credit = df_credit.merge(
        pd.get_dummies(
            df_credit[
                [
                    "Purpose",
                    "Sex",
                    "Housing",
                    "Risk",
                    "Checking account",
                    "Saving accounts",
                    "Age_cat",
                ]
            ],
            prefix=["Purpose", "Sex", "Housing", "Risk", "Check", "Savings", "Age_cat"],
            drop_first=False,
        ),
        left_index=True,
        right_index=True,
    )

    del df_credit["Saving accounts"]
    del df_credit["Checking account"]
    del df_credit["Purpose"]
    del df_credit["Sex"]
    del df_credit["Housing"]
    del df_credit["Age_cat"]
    del df_credit["Risk"]
    del df_credit["Risk_good"]
    # del df_credit["Purpose_business"]
    # del df_credit["Sex_female"]
    # del df_credit["Housing_free"]
    # del df_credit["Check_little"]
    # del df_credit["Savings_little"]
    # del df_credit["Age_cat_Student"]

    df_credit.to_csv(r"german_credit_data_PD.csv", index=False)
    # Upload to S3
    uploadKey = ds + "/german_credit_data_PD.csv"
    s3.load_file(
        bucket_name="dag-umba",
        key=uploadKey,
        filename="german_credit_data_PD.csv",
        replace=True,
    )

    # Creating the X and y variables
    df_credit["Credit amount"] = np.log(df_credit["Credit amount"])
    X = df_credit.drop("Risk_bad", 1).values
    y = df_credit["Risk_bad"].values

    # Spliting X and y into train and test version
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.25, random_state=42
    )

    ## Upload training splits to S3
    X_train_key = ds + "/trainingData/X_train.npy"
    np.save("X_train.npy", X_train)
    s3.load_file(
        bucket_name="dag-umba",
        key=X_train_key,
        filename="X_train.npy",
        replace=True,
    )
    y_train_key = ds + "/trainingData/y_train.npy"
    np.save("Y_train.npy", y_train)
    s3.load_file(
        bucket_name="dag-umba",
        key=y_train_key,
        filename="Y_train.npy",
        replace=True,
    )
    X_test_key = ds + "/trainingData/X_test.npy"
    np.save("X_test.npy", X_test)
    s3.load_file(
        bucket_name="dag-umba",
        key=X_test_key,
        filename="X_test.npy",
        replace=True,
    )
    y_test_key = ds + "/trainingData/y_test.npy"
    np.save("Y_test.npy", y_test)
    s3.load_file(
        bucket_name="dag-umba",
        key=y_test_key,
        filename="Y_test.npy",
        replace=True,
    )

    return {
        "dataKey": uploadKey,
        "X_train": X_train_key,
        "X_test": X_test_key,
        "y_train": y_train_key,
        "y_test": y_test_key,
    }
