import db
import get_data
import os
from dotenv import load_dotenv

if __name__ == "__main__":

    # Get posts for a specified user, and write to a SQL Server table
    user_handle = "tmtest.bsky.social"
    output_table = "public.skeets_stg"

    post_data = get_data.get_posts(user_handle)
    db.write_list_to_db(post_data, output_table)

