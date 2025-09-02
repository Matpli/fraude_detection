# =============================
# Importation des librairies
# =============================
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests


# =============================
# Paramètres par défaut du DAG
# =============================
default_args_notify = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 31),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# =============================
# Définition du DAG Airflow
# =============================
dag_notify = DAG(
    "fraud_detection_notification",
    default_args=default_args_notify,
    schedule_interval="*/5 * * * *",  # toutes les 5 minutes
    catchup=False,
)


# =============================
# Fonction : Notification Zapier
# =============================
def notify_fraud_zapier(**kwargs):
    # Connexion à Redshift via PostgresHook
    hook = PostgresHook(postgres_conn_id="redshift_conn_id")
    conn = hook.get_conn()
    cursor = conn.cursor()
# recherche si fraude dans les 5 dernieres entrees de la table fraud_predictions
    cursor.execute("""
        SELECT merchant, amt, city, state, prediction, job, TO_TIMESTAMP(unix_time) AS event_time
        FROM (
            SELECT *
            FROM fraud_predictions
            ORDER BY unix_time DESC
            LIMIT 5
        ) AS last_preds
        WHERE prediction = 1;
    """)
    rows = cursor.fetchall()

    if not rows:
        print("Aucune fraude détectée dans les 5 dernières prédictions.")
    else:
        # Construction du tableau HTML 
        html_table = "<table border='1'><tr><th>Merchant</th><th>Amount</th><th>City</th><th>State</th><th>Job</th><th>Prediction</th><th>Event Time</th></tr>"
        for r in rows:
            html_table += f"<tr><td>{r[0]}</td><td>{r[1]}</td><td>{r[2]}</td><td>{r[3]}</td><td>{r[5]}</td><td>{r[4]}</td><td>{r[6]}</td></tr>"
        html_table += "</table>"

        # Envoi vers Zapier
        zapier_webhook_url = "https://hooks.zapier.com/hooks/catch/24426701/uhfprab/"
        payload = {
            "alert": f"{len(rows)} fraude(s) détectée(s) parmi les 5 dernières transactions",
            "details": html_table
        }

        response = requests.post(zapier_webhook_url, json=payload)
        if response.status_code == 200:
            print("Notification envoyée à Zapier ✅")
        else:
            print(f"Erreur Zapier : {response.text}")

    # Fermeture 
    cursor.close()
    conn.close()


# =============================
# Définition de la tâche Airflow
# =============================
notify_task = PythonOperator(
    task_id="notify_fraud_zapier",
    python_callable=notify_fraud_zapier,
    provide_context=True,
    dag=dag_notify,
)

