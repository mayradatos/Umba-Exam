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
    return "Model_RF"
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

    # TODO: Upload boxplot to S3
    # boxplot algorithm comparison
    # fig = plt.figure(figsize=(11, 6))
    # fig.suptitle("Algorithm Comparison")
    # ax = fig.add_subplot(111)
    # plt.boxplot(results)
    # ax.set_xticklabels(names)
    # plt.show()  ##graficar en airflow :)

    # return "Model_" + best["name"]
    return "Model_RF"
