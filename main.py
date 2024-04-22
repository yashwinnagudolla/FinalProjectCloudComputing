import sqlite3
import mysql.connector
import pandas as pd
import shutil
import json
import os
from mysql.connector import errorcode
from flask import Flask, request, g, render_template, send_file
from werkzeug.utils import secure_filename
from decimal import *

DATABASE = "C:/Users/Yashwin Nagudolla/Downloads/Others/Others/example.db"
app = Flask(__name__)
app.config.from_object(__name__)
app.config['Upload_folder_HouseHolds']="/tmp/static/UploadFiles/Households";
app.config['Upload_folder_Transactions']="/tmp/static/UploadFiles/Transactions";
app.config['Upload_folder_Products']="/tmp/static/UploadFiles/Products";
if not os.path.exists(app.config['Upload_folder_HouseHolds']):
   os.makedirs(app.config['Upload_folder_HouseHolds'],0o777)
else:
   shutil.rmtree(app.config['Upload_folder_HouseHolds'])
   os.makedirs(app.config['Upload_folder_HouseHolds'],0o777)
if not os.path.exists(app.config['Upload_folder_Transactions']):
   os.makedirs(app.config['Upload_folder_Transactions'],0o777)
else:
    shutil.rmtree(app.config['Upload_folder_Transactions'])
    os.makedirs(app.config['Upload_folder_Transactions'],0o777)
if not os.path.exists(app.config['Upload_folder_Products']):
   os.makedirs(app.config['Upload_folder_Products'],0o777)
else:
    shutil.rmtree(app.config['Upload_folder_Products'])
    os.makedirs(app.config['Upload_folder_Products'],0o777)
    
allowed_extensions = ['csv']

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        
        if isinstance(obj, Decimal):
            return str(obj)
        return json.JSONEncoder.default(self, obj)

def check_file_extension(filename):
    return filename.split('.')[-1] in allowed_extensions

def connect_to_database():
    return sqlite3.connect(app.config['DATABASE'])

def get_db():
    db = getattr(g, 'db', None)
    if db is None:
        db = g.db = connect_to_database()
    return db

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, 'db', None)
    if db is not None:
        db.close()

def execute_query(query, args=()):
    cur = get_db().execute(query, args)
    rows = cur.fetchall()
    cur.close()
    return rows

def commit():
    get_db().commit()

@app.route('/')
def welcome():
    execute_query("DROP TABLE IF EXISTS users")
    execute_query("CREATE TABLE users (Username text,Password text,firstname text, lastname text, email text, count integer)")
    return render_template('login.html')

@app.route('/login', methods =['POST', 'GET'])
def login():
    message = ''
    if request.method == 'POST' and str(request.form['username']) !="" and str(request.form['password']) != "":
        username = str(request.form['username'])
        password = str(request.form['password'])
        conn = connecttoDataBase()
        cursor = conn.cursor()
        cursor.execute("""SELECT firstname,lastname,email,count  FROM users WHERE Username  = (%s) AND Password = (%s)""",(username, password))
        result=cursor.fetchall()
        if result:
            print("Logged in Successfully")
            return render_template('HomePage.html', message=message)
        else:
            message = 'Invalid Credentials !'
    elif request.method == 'POST':
        message = 'Please enter Credentials'
    return render_template('login.html', message=message)

@app.route('/dashboard', methods =['GET','POST'])
def dashboard():
    return render_template('HomePage.html')

@app.route('/uploaddatasets', methods =['GET','POST'])
def uploaddatasets():
    return render_template('UploadData.html')

@app.route('/registration', methods =['GET','POST'])
def registration():
    message = ''
    if request.method == 'POST' and str(request.form['username']) !="" and str(request.form['password']) !="" and str(request.form['firstname']) !="" and str(request.form['lastname']) !="" and str(request.form['email']) !="":
        username = str(request.form['username'])
        password = str(request.form['password'])
        firstname = str(request.form['firstname'])
        lastname = str(request.form['lastname'])
        email = str(request.form['email'])
        conn = connecttoDataBase()
        cursor = conn.cursor()
        result = cursor.execute("""SELECT *  FROM users WHERE Username  = (%s)""", (username, ))
        if result:
            message = 'User has already registered!'
        else:
            conn = connecttoDataBase()
            cursor = conn.cursor()
            sqlst="""INSERT INTO users (Username, Password, firstname, lastname, email) VALUES (%s, %s, %s, %s, %s)"""
            result1=cursor.execute(sqlst,
            (str(username), str(password), str(firstname), str(lastname), str(email)));
            conn.commit();
            cursor.close()
            conn.close()
            print(result1)
            print("Connection establishedddddd")
            print(username)
            print(password)
            conn = connecttoDataBase()
            cursor = conn.cursor()
            cursor.execute("""SELECT firstname,lastname,email,count  FROM users WHERE Username  = (%s) AND Password = (%s)""",(username, password))
            print("Connection establishedddddd before commit")
            result2=cursor.fetchall()
            commit()
            print(result2)
            if result2:
                message = 'Resgistration succesfully completed.'
                return render_template('registrationSuccess.html', message=message)
    elif request.method == 'POST':
        message = 'Please fill all the fields'
    return render_template('registration.html', message=message)

@app.route('/searchhhm', methods =['GET', 'POST'])
def searchhhm():
    return loadtable("53");

@app.route('/searchhhmnew', methods =['GET','POST'])
def searchhhmnew():
    if request.method == 'POST' and str(request.form['hshd_num']) != "":
        try:
            response=loadtable(str(request.form['hshd_num']));
        except:
            message = "Valid HSHD_NUM is required!!"
            return render_template('SearchHHM.html', message=message)
    else:
        message = "Valid HSHD_NUM is required!!"
        return render_template('SearchHHM.html',message=message)
    return response;

@app.route('/storeuploadedhouseholdfile', methods =['GET','POST'])
def storeuploadedhouseholdfile():
    message = 'Please upload file again!!'
    if request.method == 'POST':  
        f = request.files['file']  
        if check_file_extension(f.filename):
            f.save(os.path.join(app.config['Upload_folder_HouseHolds'],secure_filename(f.filename)))  # this will secure the file
            readCSVandloaddata(os.path.join(app.config['Upload_folder_HouseHolds'], secure_filename(f.filename)),"households");
            message='file uploaded successfully'  
        else:
            message='The file extension is not allowed'

    return render_template('UploadData.html',message=message)

@app.route('/storeuploadedProductfile', methods =['GET','POST'])
def storeuploadedProductfile():
    message = 'Please upload file again!!'
    if request.method == 'POST':  
        f = request.files['file']  
        if check_file_extension(f.filename):
            f.save(os.path.join(app.config['Upload_folder_Products'],secure_filename(f.filename)))  # this will secure the file
            readCSVandloaddata(os.path.join(app.config['Upload_folder_Products'], secure_filename(f.filename)),"products");
            message='file uploaded successfully'  
        else:
            message='The file extension is not allowed'

    return render_template('UploadData.html',messageProducts=message)

@app.route('/storeuploadedTransactionfile', methods =['GET','POST'])
def storeuploadedTransactionfile():
    message = 'Please upload file again!!'
    if request.method == 'POST':  
        f = request.files['file']  
        if check_file_extension(f.filename):
            f.save(os.path.join(app.config['Upload_folder_Transactions'], secure_filename(f.filename)))  # this will secure the file
            readCSVandloaddata(os.path.join(app.config['Upload_folder_Transactions'], secure_filename(f.filename)),"transactions");
            message='file uploaded successfully'  
        else:
            message='The file extension is not allowed'

    return render_template('UploadData.html',messageTransactions=message)



def executeselectquery(queryString):
    conn = connecttoDataBase()
    return pd.read_sql(queryString,conn)

def loadtable(hshd_num):

    conn = connecttoDataBase()
    cursor = conn.cursor()
    cursor.execute("""Select a.HSHD_NUM,b.BASKET_NUM,b.PURCHASE_,b.PRODUCT_NUM,c.DEPARTMENT,c.COMMODITY,b.SPEND,b.UNITS,b.STORE_R,b.WEEK_NUM,b.YEAR_NUM,a.L,
    a.AGE_RANGE,a.MARITAL,a.INCOME_RANGE,a.HOMEOWNER,a.HSHD_COMPOSITION,a.HH_SIZE,a.CHILDREN from households as a inner join transactions as b inner join 
    products as c on a.HSHD_NUM=b.HSHD_NUM and b.PRODUCT_NUM=c.PRODUCT_NUM where a.HSHD_NUM="""+hshd_num+" order by a.HSHD_NUM,b.BASKET_NUM,b.PURCHASE_,b.PRODUCT_NUM,c.DEPARTMENT,c.COMMODITY;");
    rows = cursor.fetchall()
    print("rows")
    print(rows)
    conn.commit()
    cursor.close()
    conn.close()
    return render_template('SearchHHMTable.html',data=rows)



def connecttoDataBase():
    config = {
    'host': 'yashwin-cloud.mysql.database.azure.com',
    'port':'3306',
    'user': 'yashwin',
    'password':'*Dead1Me2' ,
    'database':'finalproject'
    }
    try:
        conn = mysql.connector.connect(**config)
        print("Connection established")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with the user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    return conn;

def readCSVandloaddata(csvfilepath,dataFrom):
    conn = connecttoDataBase()
    cursor = conn.cursor()
    df = pd.read_csv(csvfilepath)
    df.columns = df.columns.str.replace(' ', '')
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    dftotuple = list(df.to_records(index=False))
    if(dataFrom=='households'):
        for eachtuple in dftotuple:
            try:
                cursor.execute(
                    """INSERT INTO households (HSHD_NUM,L,AGE_RANGE,MARITAL,INCOME_RANGE,HOMEOWNER,HSHD_COMPOSITION,HH_SIZE,CHILDREN) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (int(eachtuple.HSHD_NUM), str(eachtuple.L), str(eachtuple.AGE_RANGE), str(eachtuple.MARITAL),
                     str(eachtuple.INCOME_RANGE), str(eachtuple.HOMEOWNER), str(eachtuple.HSHD_COMPOSITION),
                     str(eachtuple.HH_SIZE), str(eachtuple.CHILDREN)));
            except Exception as e:  # work on python 3.x
                print('Failed to upload to ftp: ' + str(e))
    if (dataFrom == 'transactions'):
        for eachtuple in dftotuple:
            try:
                cursor.execute(
                    '''INSERT INTO transactions (BASKET_NUM,HSHD_NUM,PURCHASE_,PRODUCT_NUM,SPEND,UNITS,STORE_R,WEEK_NUM,YEAR_NUM) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                    (int(eachtuple.BASKET_NUM), int(eachtuple.HSHD_NUM), str(eachtuple.PURCHASE_),
                     int(eachtuple.PRODUCT_NUM), int(eachtuple.SPEND), int(eachtuple.UNITS), str(eachtuple.STORE_R),
                     int(eachtuple.WEEK_NUM), int(eachtuple.YEAR)));
            except Exception as e:  # work on python 3.x
                print('Failed to upload to ftp: ' + str(e))
    if (dataFrom == 'products'):
        for eachtuple in dftotuple:
            try:
                cursor.execute("""INSERT INTO products (PRODUCT_NUM,DEPARTMENT,COMMODITY,BRAND_TY,NATURAL_ORGANIC_FLAG) VALUES (%s,%s,%s,%s,%s)""",
                    (int(eachtuple.PRODUCT_NUM), str(eachtuple.DEPARTMENT), str(eachtuple.COMMODITY),str(eachtuple.BRAND_TY),
                     str(eachtuple.NATURAL_ORGANIC_FLAG)));
            except Exception as e:  # work on python 3.x
                print('Failed to upload to ftp: ' + str(e))
    conn.commit()
    cursor.close()
    conn.close()

def readCSVFileandStoretoAzure(csvfilename,dataFrom):
    conn=connecttoDataBase()
    cursor = conn.cursor()

    df = pd.read_csv("C:/Users/Yashwin Nagudolla/Downloads/Others/Others/8451_The_Complete_Journey_2_Sample/400_products.csv")
    df.columns = df.columns.str.replace(' ', '')
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    dftotuple = list(df.to_records(index=False))
    for eachtuple in dftotuple:
        try:
            cursor.execute(
                """INSERT INTO households (HSHD_NUM,L,AGE_RANGE,MARITAL,INCOME_RANGE,HOMEOWNER,HSHD_COMPOSITION,HH_SIZE,CHILDREN) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (int(eachtuple.HSHD_NUM), str(eachtuple.L), str(eachtuple.AGE_RANGE), str(eachtuple.MARITAL),
                 str(eachtuple.INCOME_RANGE), str(eachtuple.HOMEOWNER), str(eachtuple.HSHD_COMPOSITION),
                 str(eachtuple.HH_SIZE), str(eachtuple.CHILDREN)));
        except Exception as e:  # work on python 3.x
            print('Failed to upload to ftp: ' + str(e))
    conn.commit()
    cursor.close()
    conn.close()

@app.route('/loadDashboard', methods =['GET','POST'])
def loadDashboard():
    #First Graph(bar Graph)
    data=executeselectquery("Select AVG(b.SPEND) as Spent,a.AGE_RANGE as AGE from households as a inner join transactions as b on a.HSHD_NUM=b.HSHD_NUM group by a.AGE_RANGE;");
    data['Spent']=data['Spent'].astype(str);
    data['AGE'] = data['AGE'].astype(str);

    # data=executeselectquery("Select sum(a.SPEND) as Spent,a.YEAR_NUM as year from transactions as a group by a.YEAR_NUM;");
    # data['Spent']=data['Spent'].astype(str);
    # data['year']=data['year'].astype(str);

    #Second Grapgh(Bar Graph)
    Seconddata = executeselectquery(
        "Select AVG(b.SPEND) as spend,a.HH_SIZE as householdsize from households as a inner join transactions as b inner join products as c  on b.PRODUCT_NUM=c.PRODUCT_NUM and a.HSHD_NUM=b.HSHD_NUM and c.DEPARTMENT='FOOD'  group by c.DEPARTMENT,a.HH_SIZE;");
    Seconddata['spend'] = Seconddata['spend'].astype(str);
    Seconddata['householdsize'] = Seconddata['householdsize'].astype(str);

    SeconddataTwo = executeselectquery(
        "Select AVG(b.SPEND) as spend,a.HH_SIZE as householdsize from households as a inner join transactions as b inner join products as c  on b.PRODUCT_NUM=c.PRODUCT_NUM and a.HSHD_NUM=b.HSHD_NUM and c.DEPARTMENT='NON-FOOD'  group by c.DEPARTMENT,a.HH_SIZE;");
    SeconddataTwo['spend'] = SeconddataTwo['spend'].astype(str);

    Threedata = executeselectquery(
        "Select AVG(b.SPEND) as spend,c.COMMODITY as commodity  from households as a inner join transactions as b inner join products as c  on b.PRODUCT_NUM=c.PRODUCT_NUM and a.HSHD_NUM=b.HSHD_NUM and c.DEPARTMENT='FOOD'  group by c.COMMODITY;");
    Threedata['spend'] = Threedata['spend'].astype(str);
    Threedata['commodity'] = Threedata['commodity'].astype(str);

    Fourthdata = executeselectquery(
        "Select AVG(SPEND) as spend,YEAR_NUM as year from transactions as a group by a.YEAR_NUM;");
    Fourthdata['spend'] = Fourthdata['spend'].astype(str);
    Fourthdata['year'] = Fourthdata['year'].astype(str);

    return render_template("Dashboard.html",title='Graph1', max=17000,titletwo="Graph2",
                           labels=data['AGE'].values.tolist(), values=data['Spent'].values.tolist(),
                           labelstwo=Seconddata['householdsize'].values.tolist(),valuestwo=Seconddata['spend'].values.tolist(),
                           valuestwotwo=SeconddataTwo['spend'].values.tolist(),
                           titlethree="Graph3",
                           labelsthree=Threedata['commodity'].values.tolist(),
                           valuesthree=Threedata['spend'].values.tolist(),
                           titlefour="Graph4",
                           labelsfour=Fourthdata['year'].values.tolist(),
                           valuesfour=Fourthdata['spend'].values.tolist());

if __name__ == '__main__':
  app.run();