import pandas as pd
import psycopg2
import get_data
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

"""
    db.py

    This script contains functions to write data to a SQL Server database.
"""

# Get Postgres connection
def get_db_connection():

    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(
        host = os.getenv('DB_SERVER_NAME'),
        dbname = os.getenv('DB_NAME'),
        user = os.getenv('DB_USER'),
        password = os.getenv('DB_PASSWORD'),
        port = os.getenv('DB_PORT')
    )

    return conn


# Write list of posts to specified server and table
def write_list_to_db(list_data, output_table):
    
    # This script writes a list of posts to a SQL Server table. If the table already exists, it will be dropped and recreated.

        
    # Connect to SQL Server
    conn = get_db_connection()
    cursor = conn.cursor()

    # Column list vars
    colList = []
    colListWithDataType = ""


    # Create table
    firstrow = list_data[0]
    for col in firstrow.keys():
        if colListWithDataType != "":
            colListWithDataType = colListWithDataType + "\r\n\t, "            
        colListWithDataType = colListWithDataType + col + " VARCHAR"
        colList.append(col)

    # drop table if exists
    drop_sql = "DROP TABLE IF EXISTS " + output_table
    cursor.execute(drop_sql)

    
    create_sql = "CREATE TABLE " + output_table + " ("
    create_sql += "\t" + colListWithDataType
    create_sql += ")"

    cursor.execute(create_sql)
    conn.commit()

    
    # Loop through list and insert each row
    for row in list_data:

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
