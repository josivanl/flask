from flask import Flask, request
import helper
import os

app = Flask(__name__)

@app.route('/saveOrUpdateCustomer')
def saveOrUpdateCustomer():
    try:
        customer_token = request.args.get('customer_token')
        date_start = request.args.get('date_start')
        customer_name = request.args.get('customer_name')
        customer_email = request.args.get('customer_email')
        customer_cpfcnpj = request.args.get('customer_cpfcnpj')
        service_job = request.args.get('service_job')
        service_activity = request.args.get('service_activity')

        result_enabled = helper.customer_entity(customer_token, date_start, customer_name, customer_email, customer_cpfcnpj, service_job, service_activity)

        dictionary = {
            'result': 'Ok',
            'id': result_enabled[0],
            'enabled': result_enabled[1]
        }
        return helper.dictionaryToJson(dictionary)
    except (Exception) as error:
        print(error)
        dictionary = {
            'result': error.message,
            'id': '',
            'enabled': ''
        }
        # Serializing json
        return helper.dictionaryToJson(dictionary)

@app.route('/saveOrUpdateCustomerDatabase')
def saveOrUpdateCustomerDatabase():
    try:
        id_customer = request.args.get('id_customer')
        database_host = request.args.get('database_host')
        database_name = request.args.get('database_name')
        database_port = request.args.get('database_port')
        database_user = request.args.get('database_user')
        database_password = request.args.get('database_password')
        generate_key = request.args.get('generate_key')

        helper.customerDatabase_entity(id_customer, database_host, database_name, database_port, database_user, database_password, generate_key)

        dictionary = {
            'result': 'Ok'
        }
        return helper.dictionaryToJson(dictionary)
    except (Exception) as error:
        print(error)
        if error != "":
            dictionary = {
                'result': "Error: - " + str(error)
            }
        else:
            dictionary = {
                'result': error.massage
            }
        # Serializing json
        return helper.dictionaryToJson(dictionary)

@app.route('/structureDatabaseCreate')
def structureDatabaseCreate():
    try:
        return helper.structure_database_create()
    except (Exception) as error:
        print(error)
        if error != "":
            dictionary = {
                'result': "Error: - " + str(error)
            }
        else:
            dictionary = {
                'result': error.massage
            }
        # Serializing json
        return helper.dictionaryToJson(dictionary)

@app.route('/structureDatabaseCreateUpdate/<id>', methods=["PUT"])
def structureDatabaseCreateUpdate(id):
    try:
        return helper.structure_database_create_update(id)
    except (Exception) as error:
        print(error)
        if error != "":
            dictionary = {
                'result': "Error: - " + str(error)
            }
        else:
            dictionary = {
                'result': error.massage
            }
        # Serializing json
        return helper.dictionaryToJson(dictionary)

@app.route('/structureDatabaseUpdate')
def structureDatabaseUpdate():
    try:
        return helper.structure_database_update()
    except (Exception) as error:
        print(error)
        if error != "":
            dictionary = {
                'result': "Error: - " + str(error)
            }
        else:
            dictionary = {
                'result': error.massage
            }
        # Serializing json
        return helper.dictionaryToJson(dictionary)

@app.route('/structureDatabaseUpdateUpdate/<id>', methods=["PUT"])
def structureDatabaseUpdateUpdate(id):
    try:
        return helper.structure_database_create_update_update(id)
    except (Exception) as error:
        print(error)
        if error != "":
            dictionary = {
                'result': "Error: - " + str(error)
            }
        else:
            dictionary = {
                'result': error.massage
            }
        # Serializing json
        return helper.dictionaryToJson(dictionary)

@app.route('/jobMonitor/<id_customer>', methods=["POST"])
def jobMonitor(id_customer):
    try:
        content = request.json
        if len(content) > 0:
            helper.jobMonitorDelete(id_customer)
            helper.jobMonitorAdd(id_customer, content)

        dictionary = {
            'result': "Ok"
        }
        # Serializing json
        return helper.dictionaryToJson(dictionary)
    except (Exception) as error:
        print(error)
        if error != "":
            dictionary = {
                'result': "Error: - " + str(error)
            }
        else:
            dictionary = {
                'result': error.massage
            }
        # Serializing json
        return helper.dictionaryToJson(dictionary)

@app.route('/appUserAdd/<token_customer>', methods=["POST"])
def appUserAdd(token_customer):
    try:
        content = request.json
        result = helper.appUserAdd(token_customer, content)
        dictionary = {
            'result': result
        }
        # Serializing json
        return helper.dictionaryToJson(dictionary)
    except (Exception) as error:
        print(error)
        if error != "":
            dictionary = {
                'result': "Error: - " + str(error)
            }
        else:
            dictionary = {
                'result': error.massage
            }
        # Serializing json
        return helper.dictionaryToJson(dictionary)


@app.route('/appUserLogin', methods=["POST"])
def appUserLogin():
    try:
        content = request.json
        return helper.appUserFindToUser(content)

        # Serializing json
    except (Exception) as error:
        print(error)
        if error != "":
            dictionary = {
                'result': "Error: - " + str(error)
            }
        else:
            dictionary = {
                'result': error.massage
            }
        # Serializing json
        return helper.dictionaryToJson(dictionary)

if __name__ == '__main__':
    app.run(debug=True, port=os.getenv("PORT", default=5000))
