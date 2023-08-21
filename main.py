from flask import Flask, jsonify, request
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
        id_customer = request.args.get('id_costumer')
        database_host = request.args.get('database_host')
        database_name = request.args.get('database_name')
        database_port = request.args.get('database_port')
        database_user = request.args.get('database_user')
        database_password = request.args.get('database_password')

        helper.customerDatabase_entity(id, database_host, database_name, database_port, database_user, database_password)

        dictionary = {
            'result': 'Ok'
        }
        return helper.dictionaryToJson(dictionary)
    except (Exception) as error:
        print(error)
        dictionary = {
            'result': error.message
        }
        # Serializing json
        return helper.dictionaryToJson(dictionary)


if __name__ == '__main__':
    app.run(debug=True, port=os.getenv("PORT", default=5000))
