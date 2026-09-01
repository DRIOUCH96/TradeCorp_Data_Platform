import time

import psycopg2


connexion = psycopg2.connect(
    host="postgres",
    port=5432,
    database="artisell",
    user="postgres",
    password="postgres",
)

try:
    with connexion.cursor() as curseur:
        curseur.execute(
            """
            CREATE TABLE IF NOT EXISTS clients (
                id SERIAL PRIMARY KEY,
                nom VARCHAR(100) NOT NULL,
                email VARCHAR(255) UNIQUE NOT NULL,
                ville VARCHAR(100) NOT NULL
            );
            """
        )

        curseur.execute(
            """
            INSERT INTO clients (nom, email, ville)
            VALUES
                ('Alice Martin', 'alice@artisell.fr', 'Paris'),
                ('Karim Benali', 'karim@artisell.fr', 'Lyon'),
                ('Sophie Durand', 'sophie@artisell.fr', 'Marseille')
            ON CONFLICT (email) DO NOTHING;
            """
        )

    connexion.commit()
    print("Données chargées avec succès !", flush=True)

    # Garde le conteneur actif pour obtenir les trois services "running".
    while True:
        time.sleep(3600)
finally:
    connexion.close()