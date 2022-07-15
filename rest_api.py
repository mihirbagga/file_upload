from crypt import methods
from flask import Flask, render_template
import os
import json
from flask import request, flash, jsonify, abort
from sqlalchemy import false
from random import random
import logging
from flask_cors import CORS, cross_origin
from flask import send_from_directory
import pandas as pd
from connector import PostgresConnector, get_data, delete_data, add_data, update_data, login
from flask_mail import *
from random import *

app = Flask(__name__)
app.secret_key = 'cusa'
CORS(app)
app.config['UPLOAD_FOLDER'] = 'files'

mail = Mail(app)
app.config["MAIL_SERVER"] = 'smtp.gmail.com'
app.config["MAIL_PORT"] = 465
app.config["MAIL_USERNAME"] = 'mihirbagga.88@gmail.com'
app.config['MAIL_PASSWORD'] = 'Bagga707@@@'
app.config['MAIL_USE_TLS'] = False
app.config['MAIL_USE_SSL'] = True
mail = Mail(app)
otp = randint(000000, 999999)

PostgresConnector.PostgreSQL_connect()


multi_upload_html = '''<!doctype html>
        <title>Upload new logs (can upload multiple ones)</title>
        <h1>Upload new logs </h1>
        <form method=post enctype=multipart/form-data>
          <p><input type=file name=file multiple=multiple>
             <input type=submit value=Upload>
        </form>
        '''

home_page = '''<!DOCTYPE html>  
 <html>  
 <head>  
     <title>index</title>  
 </head>  
 <body>   
 <form action = "http://localhost:5000/verify" method = "POST">  
 Email address: <input type="email" name="email">  
 <input type = "submit" value="Submit">  
 </form>  
 </body>  
 </html> '''


verify_html = '''
 <!DOCTYPE html>
 <html>  
 <head>  
     <title>OTP Verification</title>  
 </head>  
   <body>  
  <form action = "/validate" method="post">   
 <h4> OTP has been sent to the email id. Please check the email for the confirmation.</h4>  
 Enter OTP(One-time password): <input type="text" name="otp">  
 <input type="submit" value="Submit">  
 </form>  
 </body>  
 </html>'''


@app.route('/get-data', methods=['GET'])
@cross_origin(origin='*')
def getData():
    table = request.args.get('table')
    df = get_data(table)
    print(input)

    output_json = {}
    output_json['data'] = df.to_dict("records")
    return jsonify(output_json), 200


@app.route('/delete', methods=['DELETE'])
@cross_origin(origin='*')
def deleteData():
    input = request.get_json(force=True)
    table = input['table']
    id = input['id']
    response = delete_data(id, table)
    if response:
        return {"message": "Deleted"}, 200
    else:
        return {"message": "Error Deleting records"}, 400


@app.route('/add-user', methods=['POST'])
@cross_origin(origin='*')
def addData():
    input = request.get_json(force=True)
    response = add_data(input['username'], input['mobile'], input['name'], input['email'],
                        input['password'], input['address'], input['firmName'], input['userType'], input['is_archived'])

    if response:
        return {"message": "Data Added Successfully"}, 200
    else:
        return {"message": "Error in adding data"}, 400


@app.route('/login', methods=['POST'])
@cross_origin(origin='*')
def Login():
    inputs = request.get_json(force=True)
    print(inputs)
    response = login(inputs['username'],
                     inputs['password'])

    if response:
        return {"message": "Login Successfully", "status": 200}, 200
    else:
        return {"message": "Error in log in", "status": 400}, 400


@app.route('/update', methods=['POST'])
@cross_origin(origin='*')
def updateData():
    input = request.get_json(force=True)
    response = update_data(input['id'], input['name'], input['table'])

    if response:
        return {"message": "Data Added Successfully"}, 200
    else:
        return {"message": "Error in adding data"}, 400


@app.route('/download/<path:filename>', methods=['GET', 'POST'])
def download(filename):
    full_path = os.path.join(app.root_path, app.config['UPLOAD_FOLDER'])
    if os.path.exists(full_path+'/'+filename):
        return send_from_directory(full_path, filename, as_attachment=True)
    else:
        return abort(400, 'No file is Present')


@app.route('/upload-file', methods=['GET', 'POST'])
def process_log23():
    print(request)
    if request.method == 'POST':
        print('in uplaod')
        if 'file' not in request.files:
            app.logger.error('No file in request')
            return abort(400, 'No file in request')
        files = request.files.getlist('file')
        for file in files:
            if file.filename == '':
                return abort(400, 'No file selected')
                # flash('No selected file')
            if file:
                app.logger.info("Processing file {}".format(file.filename))
                log_path = file.filename
                file.save(log_path)
        return 'Success'

    return multi_upload_html


@app.route('/validate-mail')
def index():
    return home_page


@app.route('/verify', methods=["POST"])
def verify():
    email = request.form["email"]
    msg = Message('OTP', sender='username@gmail.com', recipients=[email])
    msg.body = '''
        Hi,
        Verify Your email
        here is your 6 digit one time password {}
         '''.format(str(otp))
    mail.send(msg)
    return verify_html


@app.route('/validate', methods=["POST"])
def validate():
    user_otp = request.form['otp']
    if otp == int(user_otp):
        return {
            "message": 'Verified',
            "status": 200
        }
    return {
        "message": 'Failed to verify',
        "status": 404
    }


if __name__ == "__main__":
    logging.basicConfig(filename='cusa_upload.log', level=logging.DEBUG)
    app.run()
