# ArtiSell Docker

Ce projet lance un environnement data composé de PostgreSQL, pgAdmin et d'un loader Python qui crée et alimente automatiquement la table `clients`.

## Services

- PostgreSQL : localhost:5433
- pgAdmin : http://localhost:8081
- Loader Python : crée et alimente la table clients

## Prérequis

- Docker Desktop démarré
- Docker Compose disponible

## Lancer le projet

```powershell
docker compose up -d --build
```

## Vérifier les services

```powershell
docker compose ps
```

Les conteneurs attendus sont :

- `artisell_postgres`
- `artisell_pgadmin`
- `artisell_loader`

## Vérifier le chargement des données

```powershell
docker logs artisell_loader
```

Le message suivant doit apparaître :

```text
Données chargées avec succès !
```

## Connexion à PostgreSQL depuis DBeaver

- Hôte : `localhost`
- Port : `5433`
- Base de données : `artisell`
- Utilisateur : `postgres`
- Mot de passe : `postgres`

## Connexion à pgAdmin

Ouvrir <http://localhost:8081> puis utiliser :

- Email : `admin@artisell.com`
- Mot de passe : `admin`

Pour enregistrer le serveur PostgreSQL dans pgAdmin :

- Nom : `ArtiSell`
- Hôte : `postgres`
- Port : `5432`
- Base de maintenance : `artisell`
- Utilisateur : `postgres`
- Mot de passe : `postgres`

La table peut être vérifiée avec la requête suivante :

```sql
SELECT * FROM clients ORDER BY id;
```

## Arrêter le projet en conservant les données

```powershell
docker compose down
```

Le volume `pgdata` est conservé. Les données PostgreSQL restent donc disponibles au prochain lancement.

## Relancer le projet

```powershell
docker compose up -d
```

## Supprimer les conteneurs et les volumes

```powershell
docker compose down -v
```

L'option `-v` supprime les volumes, notamment `pgdata`. Les données PostgreSQL sont alors supprimées. Au prochain lancement, le loader recrée la table `clients` et insère de nouvelles lignes.

## Fichiers du projet

- `docker-compose.yml` : orchestre les trois services
- `Dockerfile` : construit l'image Python du loader
- `requirements.txt` : contient la dépendance `psycopg2-binary`
- `loader.py` : crée la table `clients` et insère trois clients
