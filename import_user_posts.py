import db
import load_data
import os
from dotenv import load_dotenv
import load_data
import sys

if __name__ == "__main__":

    # Get posts for a specified user, and write to a SQL Server table
    
    print(len(sys.argv))

    if (len(sys.argv) != 3):
        print("Usage: python import_user_posts.py <user_handle> <output_table>")
        sys.exit(1)

    user_handle = sys.argv[1]
    output_table = sys.argv[2]

    load_data.load_user_posts(user_handle, output_table)

