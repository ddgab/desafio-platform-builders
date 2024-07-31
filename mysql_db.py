import mysql.connector
from mysql.connector import Error


connect = {
    "host":"34.95.170.227",
    "port": 3306,
    "user":"teste-dados-leitura",
    "password":"o7c4Cc8NDeXYbAMH",
    "database":"teste_dados"
}

class MySQL:
    def __init__(self):
        try:
            self.conn = mysql.connector.connect(**connect)
            if self.conn.is_connected():
                print(f"Sucess Connection MySQL")
        except Error as e:
            print(f"Erro Connection MySQL: {e}")

    def __del__(self):
        if self.conn.is_connected():
            self.conn.close()
            print("Connection MySQL Closed")

    def insert_query(self, query, value):
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, value)
            self.conn.commit()
            cursor.close()
        except Error as e:
            if self.conn.is_connected():
                self.conn.close()
            error = f"{type(e).__module__}:{type(e).__name__}: {str(e).rstrip()}"
            print(error)
            return False

    def return_query(self, query, value=None):
        try:
            cursor = self.conn.cursor()
            if value:
                cursor.execute(query, value)
            else:
                cursor.execute(query)
            result_query = cursor.fetchall()
            cursor.close()
            return result_query
        except Error as e:
            error = f"{type(e).__module__}:{type(e).__name__}: {str(e).rstrip()}"
            print(error)
            return False