#!/usr/bin/env python3
import os
from superset import create_app, db

def main():
    # 1) Crea la app de Superset
    app = create_app()

    # 2) Entra en el contexto antes de tocar cualquier modelo
    with app.app_context():
        # Ahora que hay contexto, importa los modelos
        from superset.models.core import Database

        # Construye la URI de ClickHouse
        uri = (
            f"clickhouse+http://"
            f"{os.environ['DATABASE_USER']}:{os.environ['DATABASE_PASSWORD']}@"
            f"{os.environ['DATABASE_HOST']}:{os.environ['DATABASE_PORT']}/"
            f"{os.environ['DATABASE_DB']}"
        )

        session = db.session
        # Solo añade si no existe
        if not session.query(Database).filter_by(database_name="clickhouse").first():
            click_db = Database(
                database_name="clickhouse",
                sqlalchemy_uri=uri,
            )
            session.add(click_db)
            session.commit()
            print("[OK] ClickHouse database added to Superset metadata")
        else:
            print("[INFO] ClickHouse database already registered")

if __name__ == "__main__":
    main()
