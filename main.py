from flask import Flask, jsonify, request
import helper
import os
import json

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
            'enabled': result_enabled
        }
        # Serializing json
        json_object = json.dumps(dictionary, indent=4)

        return json_object
    except (Exception) as error:
        print(error)
        dictionary = {
            'result': error.message,
            'enabled': ''
        }
        # Serializing json
        json_object = json.dumps(dictionary, indent=4)
        return json_object



if __name__ == '__main__':
    app.run(debug=True, port=os.getenv("PORT", default=5000))
