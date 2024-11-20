import requests
import pandas as pd
from datetime import datetime, timedelta
import psycopg2
import db

"""
    get_data.py

    This file contains functions to allow downloading of publicly-available data from the Bluesky API.
    This uses the public functions of the API, so it doesn't require authentication.

"""

# Fetch feed using a URI
def get_user_posts(user_handle, batch_size=100):


    """
        This function fetches posts from the specified user's feed, and loads the raw data 
        into a list of dictionaries. That raw data can then be processed into columnar format
        using the process_posts function.
    """


    query_uri = "https://public.api.bsky.app/xrpc/app.bsky.feed.getAuthorfeed"


    # Start with tomorrow's data (note that the cursor goes backwards in time)
    dateCursor = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z")
    
    # Data list to accumulate results for each batch
    data = []

    batch = 1
    
    # Loop through each batch. The dateCursor variable will be updated with the next cursor value,
    #   and will be None when there are no more results to fetch.
    while (dateCursor != None):

        # Parameters for this API call. Only the cursor variable will change from one batch to the next.
        params = {
            "actor": user_handle,   # User handle (required)
            "limit": batch_size,    # Number of posts to fetch (optional)
            "cursor": dateCursor    # Cursor for pagination (optional)
        }

        param = "?actor=" + user_handle + "&limit=" + str(batch_size) + "&cursor=" + dateCursor

        # Get the data
        response = requests.get(query_uri + param) # , params = params)
        response.raise_for_status()  # Raise an error for bad status codes
        thisData = response.json()

        # Get next cursor value.
        dateCursor = thisData.get("cursor")

        # Add this batch to the output list
        data.append(thisData)

        print("Batch " + str(batch) + " (" + str(dateCursor) + ") loaded")
        batch += 1

    print(len(data))

    return data


def process_posts(raw_data):

    # Process the output data returned from the API query
    
    # List for accumulating rows of data
    output_rows = []

    for batch in raw_data:
        #i = 0      
            
        postlist = batch.get("feed")
        
        for posts in postlist:
            
            for key in posts.keys():

                if (posts[key].get("cid") != None):

                    this_row = {
                        "cid": posts[key]["cid"],
                        "uri": posts[key]["uri"],
                        "text": posts[key]["record"]["text"],
                        "author_did": posts[key]["author"]["handle"],
                        "author_name": posts[key]["author"]["displayName"],
                        "createdAt": posts[key]["record"]["createdAt"],
                        "replyCount": posts[key]["replyCount"],
                        "repostCount": posts[key]["repostCount"],
                        "likeCount": posts[key]["likeCount"],
                        "quoteCount": posts[key]["quoteCount"]
                    }

                    output_rows.append(this_row)
                    #i += 1

    return output_rows


def load_followers(user_handle):
    # Base query for follower list (public endpoint - no authentication required)
    query_uri = "https://public.api.bsky.app/xrpc/app.bsky.graph.getFollowers"

    # Variable for pagination
    paginationCursor = ""
    
    # Data list to accumulate results for each batch
    data = []
   
    
    # Loop through each batch. The dateCursor variable will be updated with the next cursor value,
    #   and will be None when there are no more results to fetch.
    while (paginationCursor != None):

        # Parameters for this API call. Only the cursor variable will change from one batch to the next.
        params = "?actor=" + user_handle + "&limit=100"

        # If 2nd or subsequent batch, add cursor to params
        if (paginationCursor != ""):
            params += "&cursor=" + paginationCursor

        # Build the URL
        thisQuery = query_uri + params
        
        # Get the data
        response = requests.get(thisQuery)
        response.raise_for_status()  # Raise an error for bad status codes
        thisData = response.json()

        # Get next cursor value.
        paginationCursor = thisData.get("cursor")

        # Add this batch to the output list
        data.append(thisData)

    # Connect to database, and create a cursor object
    conn = db.get_db_connection()
    cur = conn.cursor()

    # Delete data loaded earlier today
    delete_query = "DELETE FROM followers WHERE dateloaded = current_date"
    cur.execute(delete_query)

    # With the results from above, load data into the followers table
    for result in data:
        for key in result.keys():
            if key == "followers":
                

                # Insert the follower data into the database
                insert_query = """
                    INSERT INTO followers (user_handle, follower_did, follower_handle, follower_displayname, dateloaded)
                    VALUES (%s, %s, %s, %s, current_date)
                """

                for follower in result[key]:
                    cur.execute(insert_query, (user_handle, follower.get("did"), follower.get("handle"), follower.get("displayName")))
    
    # Commit the transaction and close the cursor and connection
    conn.commit()
    cur.close()
    conn.close()
    

    return data



def load_follows(user_handle):

    """
        Load a list of handles followed by the specified user.
    """


    # Base query for follower list (public endpoint - no authentication required)
    query_uri = "https://public.api.bsky.app/xrpc/app.bsky.graph.getFollows"

    # Variable for pagination
    paginationCursor = ""
    
    # Data list to accumulate results for each batch
    data = []
   
    
    # Loop through each batch. The dateCursor variable will be updated with the next cursor value,
    #   and will be None when there are no more results to fetch.
    while (paginationCursor != None):

        # Parameters for this API call. Only the cursor variable will change from one batch to the next.
        params = "?actor=" + user_handle + "&limit=100"

        # If 2nd or subsequent batch, add cursor to params
        if (paginationCursor != ""):
            params += "&cursor=" + paginationCursor

        # Build the URL
        thisQuery = query_uri + params
        
        # Get the data
        response = requests.get(thisQuery)
        response.raise_for_status()  # Raise an error for bad status codes
        thisData = response.json()

        # Get next cursor value.
        paginationCursor = thisData.get("cursor")

        # Add this batch to the output list
        data.append(thisData)


    # Connect to database
    conn = db.get_db_connection()

     # Create a cursor object
    cur = conn.cursor()

    # Delete data loaded earlier today (if rerunning process for a single day)
    delete_query = "DELETE FROM follows WHERE dateloaded = current_date"
    cur.execute(delete_query)

    
    # With the results from above, load data into the followers table
    for result in data:
        for key in result.keys():
            if key == "follows":
                
                # Insert the follower data into the database
                insert_query = """
                    INSERT INTO follows (user_handle, follow_did, follow_handle, follow_displayname, dateloaded)
                    VALUES (%s, %s, %s, %s, current_date)
                """

                for following in result[key]:
                    cur.execute(insert_query, (user_handle, following.get("did"), following.get("handle"), following.get("displayName")))

    # Commit the transaction
    conn.commit()

    # Close the cursor and connection
    cur.close()
    conn.close()

    return data





def load_user_detail(user_handle):

    """
        Load user detail information for the specified user, including follows, followers, and
        total number of posts.
    """

    # Base query for follower list (public endpoint - no authentication required)
    query_uri = "https://public.api.bsky.app/xrpc/app.bsky.actor.getProfile"

    # Parameters for this API call. Only the cursor variable will change from one batch to the next.
    params = "?actor=" + user_handle

    # Build the URL
    thisQuery = query_uri + params
    
    # Get the data
    response = requests.get(thisQuery)
    response.raise_for_status()  # Raise an error for bad status codes
    thisData = response.json()

    # With the results from above, load data into the followers table
    conn = db.get_db_connection()

    # Create a cursor object
    cur = conn.cursor()

    # Delete data loaded earlier today
    delete_query = "DELETE FROM user_detail WHERE handle ='" + user_handle + "' AND dateloaded = current_date"
    cur.execute(delete_query)

    # Insert the follower data into the database
    insert_query = """
        INSERT INTO user_detail (did, handle, displayname, createdat, description, followerscount, followscount, postscount, dateloaded)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, current_date)
    """

    cur.execute(insert_query, (thisData.get("did"), user_handle, thisData.get("displayName"), thisData.get("createdAt"), thisData.get("description"), thisData.get("followersCount"), thisData.get("followsCount"), thisData.get("postsCount")))

    # Commit the transaction
    conn.commit()

    # Close the cursor and connection
    cur.close()
    conn.close()
                
    return thisData




def get_posts(user_handle):
    raw_data = get_user_posts(user_handle)
    output_rows = process_posts(raw_data)   
    return output_rows
           


# Call the function and print the results
if __name__ == "__main__":

    handle = "tmtest.bsky.social"

    #load_user_detail(handle)
    #load_followers(handle)
    #load_follows(handle)
