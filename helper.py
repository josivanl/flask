import pandas as pd
import psycopg2
# from sqlalchemy import create_engine
import json

def connection():
    conn = psycopg2.connect(database="railway",
                            host="containers-us-west-208.railway.app",
                            user="postgres",
                            password="rIlX01detohjSbzMjtAv",
                            port="6034")

    return conn

def postgres_connect_engine():
    database = "railway",
    host = "containers-us-west-208.railway.app",
    user = "postgres",
    password = "rIlX01detohjSbzMjtAv",
    port = "6034"


    # str_engine = f'postgresql://{user}:{password}@{host}:{port}/{database}'
    str_engine = 'postgresql://postgres:rIlX01detohjSbzMjtAv@containers-us-west-208.railway.app:6034/railway'
    return str_engine

def dictionaryToJson(dictionary):
    json_object = json.dumps(dictionary, indent=4)
    return json_object

def customer_entity(customer_token, date_start,customer_name, customer_email, customer_cpfcnpj, service_job, service_activity):
    sql_upset = """
        INSERT INTO customer (token,date_start,name, email, cpfcnpj,service_job, service_activity, date_update)
        VALUES (
		%s,
		%s,
		%s, 
		%s, 
		%s,
		%s, 
		%s,
		now()	
		)
        ON CONFLICT (token)
        DO UPDATE SET
            (date_start,name, email, cpfcnpj,service_job, service_activity, date_update)
            = (EXCLUDED.date_start, EXCLUDED.name, EXCLUDED.email,EXCLUDED.cpfcnpj,EXCLUDED.service_job, EXCLUDED.service_activity, EXCLUDED.date_update) 
        returning id, enabled;
        """
    conn = connection()
    cursor = conn.cursor()
    cursor.execute(sql_upset, (customer_token, date_start, customer_name, customer_email, customer_cpfcnpj, service_job, service_activity))
    result_customer = cursor.fetchall()
    cursor.close()
    conn.commit()
    conn.close()

    return result_customer[0]


def customerDatabase_entity(id_customer, hostname, name, port, username, password, generate_key):
    sql_truncate = "delete from customer_database"
    conn = connection()
    cursor = conn.cursor()
    cursor.execute(sql_truncate)
    cursor.close()

    sql_upset = """
        INSERT INTO customer_database (id_customer, hostname, name, port, username, password, generate_key)
        VALUES (
		%s,
		%s,
		%s,
		%s,
		%s,
		%s,
		%s
		)
        """
    cursor_databse = conn.cursor()
    cursor_databse.execute(sql_upset, (id_customer, hostname, name, port, username, password, generate_key))
    cursor_databse.close()
    conn.commit()
    conn.close()

def job_entity(customer_id, id_job_customer, name, last_run, next_run, status, enabled):
    sql_upset = """
            INSERT INTO service_job (customer_id, id_job_customer, name, last_run, next_run, status, enabled)
            VALUES (
    		%s,
    		%s,
    		%s, 
    		%s, 
    		%s,
    		%s, 
    		%s	
    		)
            ON CONFLICT (customer_id, name)
            DO UPDATE SET
                (id_job_customer, last_run, next_run, status, enabled)
                = (EXCLUDED.id_job_customer, EXCLUDED.last_run, EXCLUDED.next_run, EXCLUDED.status, EXCLUDED.enabled);
            """
    conn = connection()
    cursor = conn.cursor()
    cursor.execute(sql_upset, (customer_id, id_job_customer, name, last_run, next_run, status, enabled))
    cursor.close()
    conn.commit()
    conn.close()

def structure_database_create():

    sql = "SELECT id, sql_text FROM structure_database WHERE indicator_created = False"
    conn = connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    result_structure = cursor.fetchall()
    cursor.close()
    conn.commit()
    conn.close()

    df = pd.DataFrame(result_structure)
    if df.size > 0:
        df.columns = ['Id', 'Sql_Text']

    return df.to_json(orient="records")

def structure_database_create_update(id):
    sql = "UPDATE structure_database SET indicator_created = True WHERE id = %s"
    conn = connection()
    cursor = conn.cursor()
    cursor.execute(sql, (id))
    cursor.close()
    conn.commit()
    conn.close()

    dictionary = {
        'result': "Ok"
    }
    # Serializing json
    return dictionaryToJson(dictionary)

def structure_database_update():

    sql = "SELECT id, sql_text, name, type_object FROM structure_database WHERE indicator_update = True"
    conn = connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    result_structure = cursor.fetchall()
    cursor.close()
    conn.commit()
    conn.close()

    df = pd.DataFrame(result_structure)
    if df.size > 0:
        df.columns = ['id', 'sql_text', 'name', 'type_object']

    return df.to_json(orient="records")


def structure_database_create_update_update(id):
    sql = "UPDATE structure_database SET indicator_update = False WHERE id = %s"
    conn = connection()
    cursor = conn.cursor()
    cursor.execute(sql, (id))
    cursor.close()
    conn.commit()
    conn.close()

    dictionary = {
        'result': "Ok"
    }
    # Serializing json
    return dictionaryToJson(dictionary)