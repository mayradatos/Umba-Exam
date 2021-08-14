FROM apache/airflow:latest
RUN pip install --no-cache-dir seaborn plotly scikit-learn scipy xgboost