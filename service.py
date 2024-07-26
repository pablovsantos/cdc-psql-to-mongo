import psycopg2
import pymongo
import time

# Configurações do PostgreSQL
pg_user = 'postgres'
pg_password = 'postgres'
pg_dbname = 'test_database'
pg_host = 'postgres-db'
pg_port = '5432'

# Configurações do MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongo-db:27017/")
mongo_db = mongo_client["test_database"]
mongo_collection = mongo_db["employees"]

def get_pg_connection():
    return psycopg2.connect(
        dbname=pg_dbname,
        user=pg_user,
        password=pg_password,
        host=pg_host,
        port=pg_port
    )

def create_trigger_and_function():
    conn = get_pg_connection()
    cursor = conn.cursor()

    cursor.execute("""
    CREATE OR REPLACE FUNCTION log_changes() RETURNS TRIGGER AS $$
    BEGIN
        IF TG_OP = 'INSERT' THEN
            INSERT INTO changes (operation, data, changed_at)
            VALUES ('INSERT', row_to_json(NEW), current_timestamp);
            RETURN NEW;
        ELSIF TG_OP = 'UPDATE' THEN
            INSERT INTO changes (operation, data, changed_at)
            VALUES ('UPDATE', row_to_json(NEW), current_timestamp);
            RETURN NEW;
        ELSIF TG_OP = 'DELETE' THEN
            INSERT INTO changes (operation, data, changed_at)
            VALUES ('DELETE', row_to_json(OLD), current_timestamp);
            RETURN OLD;
        END IF;
    END;
    $$ LANGUAGE plpgsql;

    CREATE TABLE IF NOT EXISTS changes (
        id SERIAL PRIMARY KEY,
        operation VARCHAR(10),
        data JSONB,
        changed_at TIMESTAMP
    );

    DROP TRIGGER IF EXISTS employees_changes_trigger ON employees;
    CREATE TRIGGER employees_changes_trigger
    AFTER INSERT OR UPDATE OR DELETE ON employees
    FOR EACH ROW EXECUTE FUNCTION log_changes();
    """)

    conn.commit()
    cursor.close()
    conn.close()

def get_changes():
    conn = get_pg_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, operation, data FROM changes ORDER BY id")
    changes = cursor.fetchall()
    cursor.execute("DELETE FROM changes")
    conn.commit()
    cursor.close()
    conn.close()
    return changes

def process_changes(changes):
    for change in changes:
        change_id, operation, data = change
        if operation == 'INSERT':
            mongo_collection.insert_one(data)
        elif operation == 'UPDATE':
            mongo_collection.update_one({"employee_id": data["employee_id"]}, {"$set": data})
        elif operation == 'DELETE':
            mongo_collection.delete_one({"employee_id": data["employee_id"]})

def main():
    create_trigger_and_function()
    while True:
        changes = get_changes()
        process_changes(changes)
        time.sleep(10)

if __name__ == "__main__":
    main()
