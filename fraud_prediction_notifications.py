# Importation des librairies nécessaires
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


# =============================
# Paramètres S3 et MLflow
# =============================

# Récupération du nom du bucket S3 depuis les variables Airflow
S3_BUCKET_NAME = Variable.get("S3FraudBucket")
S3_FILE_KEY = "fraudTest.csv"

# Configuration des identifiants AWS via les variables Airflow
os.environ["AWS_ACCESS_KEY_ID"] = Variable.get("AWS_ACCESS_KEY_ID")
os.environ["AWS_SECRET_ACCESS_KEY"] = Variable.get("AWS_SECRET_ACCESS_KEY")

# Configuration de l’URI de suivi MLflow
MLFLOW_TRACKING_URI = "http://192.168.1.185:5000"
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)


# =============================
# Définition du DAG de prédiction
# =============================

default_args_predict = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 31),  # Date de démarrage
    "retries": 1,  # Nombre de tentatives en cas d’échec
    "retry_delay": timedelta(minutes=5),  # Délai entre deux tentatives
}

# Définition du DAG (exécuté toutes les minutes)
dag_predict = DAG(
    "fraud_detection_fraud_prediction",
    default_args=default_args_predict,
    schedule_interval="* * * * *",  # Chaque minute
    catchup=False,
)


# =============================
# Fonction 1 : récupération + transformation des données
# =============================
def fetch_and_transform_data(**kwargs):
    url = "https://charlestng-real-time-fraud-detection.hf.space/current-transactions"
    # Appel API pour récupérer les transactions en temps réel
    response = requests.get(url)

    if response.status_code == 200:
        try:
            data = response.json()
            # Si les données sont renvoyées sous forme de chaîne JSON
            if isinstance(data, str):
                import json
                data = json.loads(data)
                print("Données brutes reçues:", data)

            # Vérifie la structure attendue des données
            if "columns" in data and "data" in data:
                # Transformation en DataFrame
                df = pd.DataFrame(data["data"], columns=data["columns"])

                # Suppression de colonnes non utiles pour la prédiction
                drop_cols = ["first", "last", "street", "zip", "dob", "trans_num", "is_fraud"]
                df.drop(columns=drop_cols, inplace=True, errors='ignore')

                # Renommage d’une colonne pour cohérence
                df.rename(columns={'current_time': 'unix_time'}, inplace=True)

                # Conversion des variables qualitatives en variables catégorielles
                categorical_cols = ["merchant", "category", "gender", "city", "state", "job"]
                for col in categorical_cols:
                    if col in df.columns:
                        df[col] = df[col].astype("category")

                # Sauvegarde dans XCom pour utilisation par la tâche suivante
                kwargs['ti'].xcom_push(key="transformed_data", value=df.to_json())
                print("Données transformées poussées dans XCom :")
                print(df.head())
            else:
                print("Erreur de format dans les données reçues.")
        except ValueError:
            print("Erreur de conversion JSON")
    else:
        print("Erreur lors de la récupération des données API")
        kwargs['ti'].xcom_push(key="transformed_data", value=None)


# =============================
# Fonction 2 : prédiction de fraude
# =============================
def predict_fraud(**kwargs):
    ti = kwargs['ti']
    # Récupération des données transformées depuis XCom
    raw_data = ti.xcom_pull(task_ids='fetch_and_transform_data', key='transformed_data')

    if raw_data is None:
        print("Aucune donnée transformée n'a été récupérée depuis XCom.")
        return
	
    transformed_data = pd.read_json(raw_data)

    # Vérifie qu’on a bien des données
    if transformed_data.empty:
        print("Les données transformées sont vides.")
        return

    # Transformation du temps en format numérique
    if 'unix_time' in transformed_data.columns:
        transformed_data['unix_time'] = transformed_data['unix_time'].astype('int64') // 10**9

    # Chargement du modèle LightGBM depuis MLflow
    model_uri = "models:/Production_fraud_lightgbm/1"
    try:
        model = mlflow.sklearn.load_model(model_uri)
        print("Modèle LightGBM chargé avec succès depuis MLflow.")
    except mlflow.exceptions.RestException as e:
        print(f"Erreur lors du chargement du modèle depuis MLflow: {e}")
        return

    # Colonnes attendues par le modèle
    model_columns = ['merchant', 'category', 'amt', 'gender', 'city', 'state', 'lat', 'long',
                     'city_pop', 'job', 'unix_time', 'merch_lat', 'merch_long']
    transformed_data = transformed_data[model_columns]

    # Transformation des colonnes catégorielles
    categorical_cols = ["merchant", "category", "gender", "city", "state", "job"]
    for col in categorical_cols:
        if col in transformed_data.columns:
            transformed_data[col] = transformed_data[col].astype("category")

    # Réalisation de la prédiction
    predictions = model.predict(transformed_data)
    transformed_data["prediction"] = predictions

    print("Prédictions en temps réel :\n", transformed_data[["prediction"]])

    # Sauvegarde des prédictions en CSV
    csv_filename = "/tmp/predictions.csv"
    transformed_data.to_csv(csv_filename, index=False)

    # Upload des résultats sur S3
    s3_hook = S3Hook(aws_conn_id="aws_conn_id")
    s3_hook.load_file(
        filename=csv_filename,
        key="predictions/predictions.csv",
        bucket_name=S3_BUCKET_NAME,
        replace=True
    )

    print("Prédictions sauvegardées sur S3 avec succès.")


# =============================
# Définition des tâches Airflow
# =============================

# Tâche 1 : récupération + transformation des données
fetch_data_task = PythonOperator(
    task_id='fetch_and_transform_data',
    python_callable=fetch_and_transform_data,
    provide_context=True,
    dag=dag_predict,
)

# Tâche 2 : prédiction avec le modèle
predict_task = PythonOperator(
    task_id='predict_fraud',
    python_callable=predict_fraud,
    provide_context=True,
    dag=dag_predict,
)

# Tâche 3 : chargement des résultats dans Redshift
upload_task = S3ToRedshiftOperator(
    task_id='upload_to_redshift',
    schema='public',
    table='fraud_predictions',
    s3_bucket=S3_BUCKET_NAME,
    s3_key='predictions/predictions.csv',
    copy_options=['CSV', 'IGNOREHEADER 1'],
    aws_conn_id='aws_conn_id',
    redshift_conn_id='redshift_conn_id',
    dag=dag_predict,
)


# =============================
# Définition de l’ordre des tâches
# =============================
fetch_data_task >> predict_task >> upload_task
