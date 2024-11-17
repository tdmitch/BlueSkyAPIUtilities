import db
import get_data
import os
from dotenv import load_dotenv

def import_user_posts_to_db(user_handle, output_table):
    post_data = get_data.get_posts(user_handle)
    db.write_posts_to_db(post_data, output_table)

if __name__ == "__main__":
    # Get posts for a specified user, and write to a SQL Server table
    import_user_posts_to_db("tmitch.net", "dbo.skeets_stg")


