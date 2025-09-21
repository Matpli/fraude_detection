# Détection de Fraude

Ce projet implémente un système complet de **détection de fraude** en utilisant des modèles d'apprentissage automatique et un orchestrateur de workflow (**Airflow**). Il inclut :

- L’entraînement et l’inférence d’un modèle de détection de fraude.  
- L’exécution de **DAGs Airflow** pour automatiser les notifications liées aux fraudes.  
- L’envoi de notifications automatisées via **Zapier** lorsque des fraudes sont détectées.  
- Le DAG **`fraud_prediction_notifications`** est **planifié toutes les minutes**, permettant un suivi quasi temps réel des fraudes détectées.

---

## 🛠️ Prérequis

- Python 3.7 ou supérieur  
- Airflow 2.x  
- Docker et Docker Compose (pour exécution conteneurisée)  
- pip (gestionnaire de paquets Python)  

---

## 📦 Installation

Clonez le dépôt et installez les dépendances :

```bash
git clone https://github.com/Matpli/fraude_detection.git
cd fraude_detection
pip install -r requirements.txt
```

## Structure du projet

| Fichier                             | Description                                                                                                                                                                              |
| ----------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `training_fraude.py`                | Script principal pour l’entraînement du modèle et la prédiction de nouvelles données. Les prédictions peuvent être effectuées ponctuellement via l’API Flask ou intégrées dans les DAGs. |
| `fraud_prediction_notifications.py` | DAG Airflow planifié toutes les minutes pour envoyer des notifications lorsqu’une fraude est détectée.                                                                                   |
| `notify_zapier.py`                  | Permet d’envoyer des notifications HTTP POST vers un webhook Zapier pour automatiser les actions suite à une fraude détectée.                                                            |
| `docker-compose.yaml`               | Configuration Docker pour exécuter Airflow                                                                                                   |
| `requirements.txt`                  | Liste des dépendances Python nécessaires.                                                                                                                                                |

## Fonctionnement des DAGs

Le DAG fraud_prediction_notifications est exécuté toutes les minutes.

À chaque exécution, il :

Récupère les nouvelles transactions ou données à analyser

Vérifie si une fraude est détectée via le modèle LightGBM (models/Production_fraud_lightgbm)

Envoie une notification si nécessaire via fraud_prediction_notifications.py et/ou notify_zapier.py

Les notifications incluent un score de fraude, une indication si c’est une fraude (true/false), et peuvent déclencher des actions automatiques via Zapier.

Exemple d’une notification envoyée :
```r
{
  "transaction_id": "12345",
  "fraud_score": 0.87,
  "is_fraud": true
}
```
