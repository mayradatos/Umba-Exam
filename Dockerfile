FROM apache/airflow:latest
RUN pip install --no-cache-dir seaborn plotly sklearn xgboost