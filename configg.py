import json
import urllib

def get_connection_config():
    producer_config = {
        'server' : 'adfsqlserverdb.database.windows.net',
        'database':'EDW',
        'username' : 'serveradmin',
        'password' : 'Shu@bham',
        'driver':'{ODBC Driver 18 for SQL Server}'
        # 'server' : 'sid-sql-server.database.windows.net',
        # 'database':'sid-sql-db',
        # 'username' : 'Siddhesh',
        # 'password' : 'Sid@1234',
        # 'driver':'{ODBC Driver 18 for SQL Server}'
    }
    return json.dumps(producer_config)


cred = json.loads(get_connection_config())
odbc_str = 'DRIVER='+cred["driver"]+';SERVER='+cred["server"]+';PORT=1433;UID='+cred["username"]+';DATABASE='+ cred["database"] + ';PWD='+ cred["password"]
connect_str = 'mssql+pyodbc:///?odbc_connect=' + urllib.parse.quote_plus(odbc_str)