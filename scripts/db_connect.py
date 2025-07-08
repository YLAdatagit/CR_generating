import psycopg2

def connect_postgres(host, port, database, user, password):
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        print("Connected to PostgreSQL database!")
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None
