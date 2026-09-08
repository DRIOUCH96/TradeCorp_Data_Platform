# TradeCorp Data Platform — Jalon 2, Partie 2

## Objectif

Ce projet transforme le prototype réalisé avec des notebooks en un pipeline PySpark modulaire, testable et exécutable depuis un terminal.

Le pipeline réalise les étapes suivantes :

1. téléchargement des huit CSV métier depuis la zone `raw` d’ADLS Gen2 ;
2. nettoyage et typage des données ;
3. jointure des sept tables utiles ;
4. ajout de la devise du client et conversion du sous-total ;
5. écriture du résultat en Parquet dans la zone `clean`.

## Architecture

```text
jalon2_tradecorp/
├── data/
│   └── raw/
│       └── reference/
│           └── country_currency.csv
├── src/
│   ├── utils.py
│   ├── reader.py
│   ├── transformer.py
│   ├── enrichment.py
│   ├── writer.py
│   ├── pipeline.py
│   ├── fetch_exchange_rates.py
│   └── upload_country_currency.py
├── tests/
│   ├── test_transformers.py
│   └── run_tests.py
├── .env.example
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

## Responsabilité des modules

- `utils.py` : connexion à ADLS et fonctions de nettoyage.
- `reader.py` : téléchargement et lecture des données.
- `transformer.py` : jointures et construction du DataFrame métier.
- `enrichment.py` : ajout de `currency` et `sous_total_local`.
- `writer.py` : écriture Parquet et upload vers la zone `clean`.
- `pipeline.py` : orchestration, journalisation et gestion des erreurs.
- `fetch_exchange_rates.py` : récupération quotidienne des taux de change.
- `upload_country_currency.py` : upload unique du mapping pays-devise.

## Configuration

Créer `.env` à partir de `.env.example`, puis renseigner les accès Azure :

```dotenv
AZURE_STORAGE_ACCOUNT_NAME=nom_du_compte
AZURE_STORAGE_ACCOUNT_KEY=cle_du_compte
AZURE_RAW_CONTAINER=raw
AZURE_RAW_REFERENCE_PATH=reference
AZURE_CLEAN_CONTAINER=clean
CLEAN_OUTPUT_PATH=orders_enriched.parquet
LOCAL_TMP_DIR=/home/jovyan/data/tmp
```

Le fichier `.env` ne doit jamais être versionné.

## Construction du conteneur

```powershell
docker compose up -d --build
docker compose ps
```

Le conteneur Spark utilisé par les commandes est :

```text
tradecorp_spark
```

## Upload initial du mapping pays-devise

Cette commande ne doit être exécutée qu’une seule fois :

```powershell
docker exec tradecorp_spark python /home/jovyan/src/upload_country_currency.py
```

Le fichier est envoyé vers :

```text
raw/reference/country_currency.csv
```

## Mise à jour quotidienne des taux

Le script utilise l’API suivante :

```text
https://api.exchangerate-api.com/v4/latest/USD
```

Exécution :

```powershell
docker exec tradecorp_spark python /home/jovyan/src/fetch_exchange_rates.py
```

La réponse JSON brute est envoyée vers :

```text
raw/reference/exchange_rates.json
```

## Validation du lecteur

```powershell
docker exec tradecorp_spark spark-submit /home/jovyan/src/reader.py
```

Le lecteur télécharge explicitement les huit fichiers métier :

- `categories.csv`
- `customers.csv`
- `employees.csv`
- `order_details.csv`
- `orders.csv`
- `products.csv`
- `shippers.csv`
- `suppliers.csv`

Il télécharge également les deux fichiers du dossier `reference`.

## Exécution des tests

Les tests doivent être lancés avec `spark-submit` :

```powershell
docker exec tradecorp_spark spark-submit /home/jovyan/tests/run_tests.py
```

Résultat attendu :

```text
4 passed
```

Les quatre tests vérifient :

- la suppression des commandes sans date de livraison ;
- le calcul de `sous_total` ;
- le nettoyage des clients ;
- l’enrichissement monétaire avec des taux simulés.

## Exécution du pipeline

Avant le pipeline, vérifier que les deux fichiers de référence sont présents dans ADLS.

```powershell
docker exec tradecorp_spark spark-submit /home/jovyan/src/pipeline.py
```

Ordre d’exécution :

```text
lecture → transformation → enrichissement → écriture
```

La SparkSession est arrêtée dans tous les cas, y compris lorsqu’une erreur survient.

## Résultat

Le résultat est écrit dans :

```text
clean/orders_enriched.parquet
```

Le schéma final contient notamment :

```text
order_id
customer_id
employee_id
product_id
order_date
required_date
shipped_date
freight
is_shipped
prix_unitaire
quantite
discount
sous_total
customer_name
customer_country
customer_city
product_name
category_name
en_stock
full_name
shipper_name
currency
sous_total_local
```

## Arrêt des conteneurs

```powershell
docker compose down
```
## Extraits des logs Airflow

### Récupération des taux de change

```text
Nombre de devises récupérées : 166
Taux de change déposés dans raw/reference/exchange_rates.json
```

### Écriture du résultat dans ADLS

```text
Parquet intermédiaire chargé : 2082 lignes
Fichier Parquet envoyé dans le conteneur clean
Upload ADLS terminé avec succès : clean/orders_enriched
```

### Capture des logs de la tâche writer

![Upload ADLS depuis Airflow](captures/05-airflow-writer-upload-log.png)
## Vérification de l’idempotence

Le DAG a été exécuté deux fois consécutivement.

Avant chaque upload, `writer.py` supprime les blobs présents sous
`clean/orders_enriched/`. Le DataFrame est écrit avec `coalesce(1)`.

Après la deuxième exécution, le dossier contient toujours un seul
fichier `part-....parquet`. Le pipeline est donc idempotent.
## Planification automatique du DAG

Le DAG `tradecorp_etl_pipeline` utilise la planification suivante :

```python
schedule_interval="0 6 * * *"
start_date=datetime(2024, 1, 1)
catchup=False
![Prochaine exécution planifiée](captures/07-airflow-next-run.png)
```

La valeur `start_date` indique la date à partir de laquelle Airflow peut
planifier le DAG. Elle ne déclenche pas automatiquement toutes les
exécutions comprises entre cette date et la date d'activation du DAG.

L'option `catchup=False` désactive le rattrapage des exécutions passées.
Ainsi, malgré une `start_date` fixée au 1er janvier 2024, Airflow n'a pas
créé une exécution pour chaque journée écoulée. Seules les exécutions
courantes et futures sont planifiées.

Le planning `0 6 * * *` lance le DAG quotidiennement à 06:00 dans le
fuseau horaire d'Airflow. Notre environnement utilise UTC.

Prochaine exécution observée : 2026-09-08T06:00:00+00:00.
## Bonus : dossier de déclenchement FileSensor

Le dossier local `data/trigger` est monté dans le conteneur Airflow à
l'emplacement `/opt/airflow/data/trigger`.

Ce montage permet au futur `FileSensor`, exécuté par Airflow, de détecter
un fichier créé depuis la machine hôte.