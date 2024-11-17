import requests
import pandas as pd
from datetime import datetime, timedelta

"""
    get_data.py

    This file contains functions to allow downloading of publicly-available data from the Bluesky API.
    This uses the public functions of theAPI, so it doesn't require authentication.


"""

# Fetch feed using a URI
def get_user_posts(user_handle, batch_size=100):

    # This function fetches posts from the specified user's feed, and loads the raw data 
    # into a list of dictionaries. That raw data can then be processed into columnar format
    # using the process_posts function.

    query_uri = "https://public.api.bsky.app/xrpc/app.bsky.feed.getAuthorfeed"


    # Start with tomorrow's data
    dateCursor = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z")
    
    # Data list to accumulate results for each batch
    data = []

    
    # Loop through each batch. The dateCursor variable will be updated with the next cursor value,
    #   and will be None when there are no more results to fetch.
    while (dateCursor != None):

        # Parameters for this API call. Only the cursor variable will change from one batch to the next.
        params = {
            "actor": user_handle,   # User handle (required)
            "limit": batch_size,    # Number of posts to fetch (optional)
            "cursor": dateCursor    # Cursor for pagination (optional)
        }

        # Get the data
        response = requests.get(query_uri, params = params)
        response.raise_for_status()  # Raise an error for bad status codes
        thisData = response.json()

        # Get next cursor value.
        dateCursor = thisData.get("cursor")

        # Add this batch to the output list
        data.append(thisData)

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
        


def get_posts(user_handle):
    raw_data = get_user_posts(user_handle)
    output_rows = process_posts(raw_data)   
    return output_rows
           


# Call the function and print the results
if __name__ == "__main__":
    data = get_posts("tmtest.bsky.social")

    print(data)
    