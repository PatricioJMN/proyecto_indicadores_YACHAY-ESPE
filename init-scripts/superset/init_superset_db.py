#!/usr/bin/env python3
import os
from superset import create_app, db


def main():
    # 1) Crea la app de Superset
    app = create_app()

    # 2) Proporciona contexto de aplicación para imports de modelos
    with app.app_context():
        # Import dinámico de Database para evitar errores de contexto
        from superset.models.core import Database
        
        # --- Añadir ClickHouse como BD ---
        uri = (
            f"clickhouse+http://"
            f"{os.environ['DATABASE_USER']}:{os.environ['DATABASE_PASSWORD']}@"
            f"{os.environ['DATABASE_HOST']}:{os.environ['DATABASE_PORT']}/"
            f"{os.environ['DATABASE_DB']}"
        )
        session = db.session
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

        # --- Configuración de roles y usuarios ---
        sm = app.appbuilder.sm

        # 1) Rol "Public": solo permisos de visualización de dashboards
        public_role = sm.find_role('Public')
        view_permissions = [
            ('menu_access', 'Dashboard'),
            ('can_list', 'DashboardModelView'),
            ('can_show', 'DashboardModelView'),
            ('can_explore', 'DashboardModelView'),
        ]
        for perm, view in view_permissions:
            sm.add_permission_role(public_role, perm, view)
        print("[OK] 'Public' role configured with view-only dashboard permissions")

        # 2) Rol "DashboardAdmin": permisos de administración de dashboards
        dash_admin = sm.find_role('DashboardAdmin') or sm.add_role('DashboardAdmin')
        admin_permissions = [
            ('all_dashboard_access', 'all_dashboard_access'),
            ('all_datasource_access', 'all_datasource_access'),
        ]
        for perm, view in admin_permissions:
            sm.add_permission_role(dash_admin, perm, view)
        print("[OK] 'DashboardAdmin' role created/updated")

        # 3) Rol "SQL_User": permisos para usar SQL Lab
        sql_role = sm.find_role('SQL_User') or sm.add_role('SQL_User')
        sql_permissions = [
            ('sql_json', 'sql_json'),
            ('all_query_access', 'all_query_access'),
        ]
        for perm, view in sql_permissions:
            sm.add_permission_role(sql_role, perm, view)
        print("[OK] 'SQL_User' role created/updated")

        # 4) Crear usuarios con los roles definidos
        def ensure_user(username, first, last, email, role, password):
            user = sm.find_user(username=username)
            if not user:
                sm.add_user(
                    username=username,
                    first_name=first,
                    last_name=last,
                    email=email,
                    role=role,
                    password=password
                )
                print(f"[OK] User {username} created with role {role.name}")
            else:
                print(f"[INFO] User {username} already exists")

        ensure_user(
            'dashboard_admin', 'Dashboard', 'Admin1', 'dash_admin@example.com',
            dash_admin, 'dashadmin_pass'
        )
        ensure_user(
            'sql_user', 'SQL_User', 'UserSQL', 'sql_user@example.com',
            sql_role, 'sqluser_pass'
        )

if __name__ == "__main__":
    main()
