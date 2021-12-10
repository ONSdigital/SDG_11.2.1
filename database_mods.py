# def create_db_connection(host_name, user_name, user_password, db_name):
#     """Connecting to SQL Server (probably MySQL eventually BigQuery)"""
#     connection = None
#     try:
#         connection = mysql.connector.connect(
#             host=host_name,
#             user=user_name,
#             passwd=user_password,
#             database=db_name
#         )
#         print("MySQL Database connection successful")
#     except Error as err:
#         print(f"Error: '{err}'")

#     return connection

# def populate_table():
#     """Populating the SQL Tables"""


# def read_query(connection, query):
#     """Reading Data formatting Output into a pandas DataFrame"""
#     cursor = connection.cursor()
#     result = None
#     try:
#         cursor.execute(query)
#         result = cursor.fetchall()
#         # Turn into df
#         return result
#     except Error as err:
#         print(f"Error: '{err}'")