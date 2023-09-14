import pandas as pd
import psycopg2
# from sqlalchemy import create_engine
from cryptography.fernet import Fernet

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
    return json_object.encode('utf-8').decode('unicode-escape')

def get_key():
    return Fernet.generate_key()

def crypt(password, key):
    f = Fernet(key)
    password_bytes = password.encode('utf-8')
    password_crypt = f.encrypt(password_bytes)
    return password_crypt

def decrypt(password_crypt, key):
    f = Fernet(key)
    password_bytes = f.decrypt(password_crypt)
    password = password_bytes.decode('utf-8')
    return password

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
        df.columns = ['id', 'sql_text']

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

def jobMonitorDelete(id_customer):
    sql = "DELETE FROM service_job WHERE customer_id = %s"
    conn = connection()
    cursor = conn.cursor()
    cursor.execute(sql, (id_customer))
    cursor.close()
    conn.commit()
    conn.close()

def jobMonitorAdd(id_customer, job_json):

    conn = connection()
    cursor = conn.cursor()
    json_data = json.loads(job_json)

    i = 0
    while i < len(json_data):
        id_job_customer = json_data[i]["CodigoJob"]
        name = json_data[i]["Nome"]
        last_run = json_data[i]["DataUltimaExecucao"]
        next_run = json_data[i]["DataProximaExecucao"]
        status = json_data[i]["Situacao"]
        enabled = False
        if json_data[i]["IndicadorJobHabilitado"] == "Sim":
            enabled = True

        sql = "INSERT INTO service_job (customer_id, id_job_customer, name, last_run, next_run, status, enabled, date_update) values (%s, %s, %s, %s, %s, %s, %s, now())"
        cursor.execute(sql, (id_customer, id_job_customer, name, last_run, next_run, status, enabled))
        i += 1

    cursor.close()
    conn.commit()
    conn.close()

def customerFindIdToToken(token):
    conn = connection()
    cursor_token = conn.cursor()

    sql_token = "SELECT id, enabled FROM customer WHERE token = %s"
    cursor_token.execute(sql_token, [token])
    result_customer = cursor_token.fetchone()
    cursor_token.close()
    conn.commit()
    conn.close()

    return  result_customer

def customerFindIdToEmail(email):
    conn = connection()
    cursor = conn.cursor()

    sql = "SELECT id, email, enabled, token, password, name FROM customer_app_user WHERE email = %s"
    cursor.execute(sql, [email])
    result_customer = cursor.fetchone()
    cursor.close()
    conn.commit()
    conn.close()

    return result_customer

def appUserAdd(token, user_json):
    customer = customerFindIdToToken(token)
    if customer is not None:
        if int(customer[0]) > 0:
            id_customer = customer[0]
            enabled = customer[1]

            if enabled == False:
                return "Cliente desativado"
            else:
                json_data = user_json

                conn = connection()
                cursor = conn.cursor()

                email = json_data["email"]
                if email == "":
                    return "E-mail inválido"
                else:
                    customer = customerFindIdToEmail(email)
                    if customer is not None:
                        if customer[2] == False:
                            return "Usuário cadastrado e desativado"
                        else:
                            return "Usuário já cadastrado"
                    else:
                        name = json_data["name"]
                        password = json_data["password"]
                        key = get_key()
                        password_crypt = crypt(password, key)
                        key_string = key.decode('utf-8')
                        password_crypt_string = password_crypt.decode('utf-8')

                        sql = "INSERT INTO customer_app_user (customer_id, name, email, password, token, date_created, date_update, enabled) values (%s, %s, %s, %s, %s, now(), now(), True)"
                        cursor.execute(sql, (id_customer, name, email, password_crypt_string, key_string))
                        cursor.close()
                        conn.commit()
                        conn.close()

                        return "Cadastro efetuado com sucesso!"

    else:
        return "Cliente não encontrado/cadastrado"

def appUserFindToUser(user_json):
    email = str(user_json["email"])
    password = str(user_json["password"])

    if email != "" and password != "":
        customer = customerFindIdToEmail(email)
        if customer is not None:
            key = bytes(customer[3], 'utf-8')
            password_decrypt = decrypt(customer[4], key)

            if password_decrypt == password:
                dictionary = {
                    'result': "Ok",
                    "id": customer[0],
                    "name": customer[5]
                }
                return dictionaryToJson(dictionary)
            else:
                dictionary = {
                    'result': "NOk",
                    "id": "0",
                    "name": ""
                }
                return dictionaryToJson(dictionary)
        else:
            dictionary = {
                'result': "NOk",
                "id": "0",
                "name": ""
            }
            return dictionaryToJson(dictionary)






