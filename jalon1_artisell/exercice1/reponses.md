# Réponses exercice 1

## Q2 — hello-world

Docker télécharge l'image hello-world si elle n'est pas présente, crée un
conteneur, exécute son programme, affiche un message de confirmation puis
arrête le conteneur.
## Q10 — disparition de la table

La table test disparaît parce qu'elle était stockée dans le système de
fichiers du conteneur. La commande docker rm supprime ce conteneur et aucun
volume n'avait été associé au dossier de données de PostgreSQL.