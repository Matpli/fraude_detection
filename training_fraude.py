import pandas as pd
import numpy as np
import requests
import mlflow
import mlflow.sklearn

from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from io import StringIO
import os
from lightgbm import LGBMClassifier


# Paramètres S3 et MLflow
S3_BUCKET_NAME = Variable.get("S3FraudBucket")
S3_FILE_KEY = "fraudTest.csv"

os.environ["AWS_ACCESS_KEY_ID"] = Variable.get("AWS_ACCESS_KEY_ID")
os.environ["AWS_SECRET_ACCESS_KEY"] = Variable.get("AWS_SECRET_ACCESS_KEY")

# Config MLflow
MLFLOW_TRACKING_URI = "http://192.168.1.185:5000"
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)


# =========================
# DAG 1 : Entraînement du modèle
# =========================
default_args_train = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 31),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag_train = DAG(
    "fraud_detection_train_xgboost",
    default_args=default_args_train,
    schedule_interval="@monthly",
    catchup=False,
)


def download_and_train(**kwargs):
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    import pandas as pd
    import numpy as np
    from io import StringIO
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import accuracy_score, classification_report
    import mlflow
    import mlflow.sklearn
    from lightgbm import LGBMClassifier

    # Récupération des données depuis S3
    s3_hook = S3Hook(aws_conn_id="aws_default")
    fraud_content = s3_hook.read_key(key=S3_FILE_KEY, bucket_name=S3_BUCKET_NAME)
    df = pd.read_csv(StringIO(fraud_content), encoding='utf-8')

    # Colonnes à supprimer
    drop_cols = ["trans_date_trans_time", "first", "last", "street", "zip", "dob", "trans_num", "Unnamed: 0"]
    df.drop(columns=drop_cols, inplace=True, errors='ignore')

    # Séparation features / cible
    X = df.drop(columns=["is_fraud"], errors='ignore')
    y = df["is_fraud"]

    # Colonnes catégorielles
    categorical_cols = ["merchant", "category", "gender", "city", "state", "job"]
    for col in categorical_cols:
        if col in X.columns:
            X[col] = X[col].astype("category")

    # Split train/test
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    # Expérience MLflow
    mlflow.set_experiment("fraudDetection")

    with mlflow.start_run():
        model = LGBMClassifier()
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)
        report = classification_report(y_test, y_pred, output_dict=True)

        mlflow.log_metric("accuracy", accuracy)
        mlflow.log_metrics({
            "precision_0": report["0"]["precision"],
            "recall_0": report["0"]["recall"],
            "f1-score_0": report["0"]["f1-score"],
            "precision_1": report["1"]["precision"],
            "recall_1": report["1"]["recall"],
            "f1-score_1": report["1"]["f1-score"],
        })

        mlflow.log_params({"test_size": 0.2, "random_state": 42})
        mlflow.sklearn.log_model(model, "lightgbm_model_fraud")

        print(f"Modèle LightGBM enregistré avec accuracy: {accuracy}")


train_task = PythonOperator(
    task_id='download_and_train',
    python_callable=download_and_train,
    dag=dag_train,
)

