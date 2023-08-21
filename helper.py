# import pandas as pd
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
    conn.commit()
    conn.close()

    return result_customer[0]


def customerDatabase_entity(id_customer, hostname, name, port, username, password):
    sql_upset = """
        INSERT INTO customer_database (id_customer, hostname, name, port, username, password)
        VALUES (
		%s,
		%s,
		%s,
		%s,
		%s,
		%s
		)
        ON CONFLICT (token)
        DO UPDATE SET
            (id_customer, hostname, name, port, username, password)
            = (EXCLUDED.id_customer, EXCLUDED.hostname, EXCLUDED.name, EXCLUDED.port, EXCLUDED.username, EXCLUDED.password) 
        """
    conn = connection()
    cursor = conn.cursor()
    cursor.execute(sql_upset, (id_customer, hostname, name, port, username, password))
    conn.commit()
    conn.close()

