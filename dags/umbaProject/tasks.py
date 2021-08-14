from pandas.io.parsers import read_csv
from airflow.decorators import task
from airflow.hooks.S3_hook import S3Hook

import pandas as pd
import numpy as np


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
    return "Model_XGB"
    ti = kwargs["ti"]
    # Get key from the PrepareData task
    X_train_key = ti.xcom_pull(task_ids="PrepareData", key="X_train")
    Y_train_key = ti.xcom_pull(task_ids="PrepareData", key="y_train")

    # Download from S3
    s3 = S3Hook(aws_conn_id="custom_s3")
    X_train_path = s3.download_file(key=X_train_key, bucket_name="dag-umba")
    Y_train_path = s3.download_file(key=Y_train_key, bucket_name="dag-umba")

    # Load Numpy arrays
    X_train = np.load(X_train_path)
    y_train = np.load(Y_train_path)

    from sklearn.model_selection import (
        KFold,
        cross_val_score,
    )  # to split the data

    from sklearn.metrics import (
        accuracy_score,
        confusion_matrix,
        classification_report,
        fbeta_score,
        recall_score,
    )  # To evaluate our model

    from sklearn.model_selection import GridSearchCV
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.linear_model import LogisticRegression
    from sklearn.tree import DecisionTreeClassifier
    from sklearn.neighbors import KNeighborsClassifier
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
    from sklearn.naive_bayes import GaussianNB
    from sklearn.svm import SVC
    from xgboost import XGBClassifier

    models = []
    models.append(("LR", LogisticRegression()))
    models.append(("LDA", LinearDiscriminantAnalysis()))
    models.append(("KNN", KNeighborsClassifier()))
    models.append(("CART", DecisionTreeClassifier()))
    models.append(("NB", GaussianNB()))
    models.append(("RF", RandomForestClassifier()))
    models.append(("SVM", SVC(gamma="auto")))
    models.append(("XGB", XGBClassifier()))

    # Evaluate each model in turn
    results = {}
    names = []
    scoring = "recall"

    for name, model in models:
        kfold = KFold(n_splits=10)
        cv_results = cross_val_score(model, X_train, y_train, cv=kfold, scoring=scoring)
        results[name] = cv_results
        names.append(name)
        msg = "%s: %f (%f)" % (name, cv_results.mean(), cv_results.std())
        print(msg)

    ## Remove models which are not implemented
    best = {"name": "", "score": 0}
    notImplemented = ["LR", "LDA", "KNN", "CART", "SVM"]
    list(map(results.pop, notImplemented))
    for name, result in results.items():
        currentScore = result.mean() - result.std()
        if currentScore > best["score"]:
            best["score"] = currentScore
            best["name"] = name

    # Generate Boxplot comparing models
    fig = plt.figure(figsize=(11, 6))
    fig.suptitle("Algorithm Comparison")
    ax = fig.add_subplot(111)
    plt.boxplot(results)
    ax.set_xticklabels(names)
    plt.savefig("algorithm_comparison.png")
    # Upload to S3
    ds = kwargs["execution_date"].strftime("%Y-%m-%d-%H-%M-%S")
    s3.load_file(
        bucket_name="dag-umba",
        key=ds + "/algorithm_comparison.png",
        filename="algorithm_comparison.png",
        replace=True,
    )

    return "Model_" + best["name"]


# Create Final node which depends on any one of the models not failing
@task(trigger_rule="none_failed")
def WriteOutput(**kwargs):
    from sklearn.metrics import (
        accuracy_score,
        confusion_matrix,
        classification_report,
        fbeta_score,
        recall_score,
    )  # To evaluate our model

    s3 = S3Hook(aws_conn_id="custom_s3")

    ti = kwargs["ti"]
    y_test_key = ti.xcom_pull(task_ids="PrepareData", key="y_test")
    cleanDataKey = ti.xcom_pull(task_ids="PrepareData", key="dataKey")
    pipelineResult = ti.xcom_pull(task_ids="Custom_Pipeline")
    selectedModel = ti.xcom_pull(task_ids="SelectModel")
    modelResults = ti.xcom_pull(task_ids=selectedModel)
    X_test_key = ti.xcom_pull(task_ids="PrepareData", key="X_test")

    X_test_path = s3.download_file(key=X_test_key, bucket_name="dag-umba")
    X_test = np.load(X_test_path)

    print("Received:" + str(modelResults))
    print("Pipeline Result:" + str(pipelineResult))

    y_pred_pipeline_path = s3.download_file(
        bucket_name="dag-umba",
        key=pipelineResult["y_pred"],
    )
    y_pred_pipeline = np.load(y_pred_pipeline_path)

    y_pred_model_path = s3.download_file(
        bucket_name="dag-umba",
        key=modelResults["y_pred"],
    )
    y_pred_model = np.load(y_pred_model_path)

    y_test_path = s3.download_file(
        bucket_name="dag-umba",
        key=y_test_key,
    )
    y_test = np.load(y_test_path)

    # Evaluate the model using fbeta_score with beta of 1
    modelScore = fbeta_score(y_test, y_pred_model, beta=1)
    pipelineScore = fbeta_score(y_test, y_pred_pipeline, beta=1)

    print("Model Score:" + str(modelScore))
    print("Pipeline Score:" + str(pipelineScore))
    # Get Y_pred from best model
    best_y_pred = y_pred_pipeline if pipelineScore > modelScore else y_pred_model
    # Delete ROC from S3 of worse model
    s3.delete_object(
        Bucket="dag-umba",
        Key=modelResults["roc_curve"]
        if modelScore < pipelineScore
        else pipelineResult["roc_curve"],
    )
    # Download data to populate headers
    cleanData_path = s3.download_file(key=cleanDataKey, bucket_name="dag-umba")
    cleanData = pd.read_csv(cleanData_path).drop("Risk_bad", 1)
    # Add a column to X_test with the predicted probability and convert to dataframe
    df = pd.DataFrame(np.hstack((X_test, best_y_pred[:, None])))
    # Save to S3
    header = list(cleanData.columns)
    header.append("Predicted_Risk_Bad")
    df.to_csv("FinalOutput.csv", index=True, header=header)
    ds = kwargs["execution_date"].strftime("%Y-%m-%d-%H-%M-%S")
    s3.load_file(
        bucket_name="dag-umba",
        key=ds + "/FinalOutput.csv",
        filename="FinalOutput.csv",
        replace=True,
    )
    with open("FinalOutput.txt", "w", encoding="utf-8") as f:
        f.write("Accuracy Score:\n")
        f.write(str(accuracy_score(y_test, best_y_pred)))
        f.write("\nConfusion matrix:\n")
        f.write(str(confusion_matrix(y_test, best_y_pred)))
        f.write("\nBeta Score:\n")
        f.write(str(pipelineScore) if pipelineScore > modelScore else str(modelScore))
        f.write("\nClassification Report:\n")
        f.write(str(classification_report(y_test, best_y_pred)))
        f.write("\nRecall Score:\n")
        f.write(str(recall_score(y_test, best_y_pred)))
        f.write(modelResults["details"])
        f.write("\n\n")

    s3.load_file(
        bucket_name="dag-umba",
        key=ds + "/FinalOutput.txt",
        filename="FinalOutput.txt",
        replace=True,
    )

    # Write ROC
