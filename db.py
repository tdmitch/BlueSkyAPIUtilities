import pandas as pd
import pyodbc
import get_data
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

"""
    db.py

    This script contains functions to write data to a SQL Server database.
"""

# Get SQL Server connection string
def get_conn_string(server, db):
    conn_str = (
        r'DRIVER={ODBC Driver 17 for SQL Server};'
        r'SERVER=' + server + ';'
        r'DATABASE=' + db + ';'
        r'Trusted_Connection=yes;'
    )
    return conn_str


# Write list of posts to specified server and table
def write_posts_to_db(post_data, output_table):

    # This script writes a list of posts to a SQL Server table. If the table already exists, it will be dropped and recreated.

    load_dotenv()
    DB_SERVER_NAME = os.getenv('DB_SERVER_NAME')
    DB_NAME = os.getenv('DB_NAME')

    
    # Connect to SQL Server
    conn_str = get_conn_string(DB_SERVER_NAME, DB_NAME)
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Column list vars
    colList = []
    colListWithDataType = ""


    # Create table
    firstrow = post_data[0]
    for col in firstrow.keys():
        if colListWithDataType != "":
            colListWithDataType = colListWithDataType + "\r\n\t, "            
        colListWithDataType = colListWithDataType + col + " NVARCHAR(500)"
        colList.append(col)

    create_sql = "IF OBJECT_ID('" + output_table + "', 'U') IS NOT NULL\r\n\tDROP TABLE " + output_table + ";\r\n\r\n"
    create_sql += ""
    create_sql += "CREATE TABLE " + output_table + " ("
    create_sql += "\t" + colListWithDataType
    create_sql += ")"

    cursor.execute(create_sql)

    
    # Loop through list and insert each row
    for row in post_data:

        # First column = don't use comma separator
        isFirst = True
        
        # Set up insert and values variables
        insert_sql = "INSERT INTO " + output_table + " (\r\n"
        values_sql = "\r\nVALUES ("

        # Loop through columns and add to insert and values variables
        for thisCol in colList:
            if isFirst == False:
                insert_sql += ", "
                values_sql += ", "
            insert_sql += thisCol
            values_sql += "'" + str(row[thisCol]).replace("'", "''") + "'"
            isFirst = False

        # Add closing parentheses
        insert_sql += ")"
        values_sql += ")"        

        # Final SQL statement    
        sql = insert_sql + values_sql

        # Execute SQL
        cursor.execute(sql)
    
    conn.commit()
    cursor.close()
    conn.close()



if __name__ == "__main__":

    # Output table name
    table_name = "dbo.skeets_stg"

    # Get data
    post_data = get_data.get_user_posts("tmtest.bsky.social")

    # Write to DB
    write_posts_to_db(post_data, table_name)
    