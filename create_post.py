import requests
import json
from datetime import datetime, timezone
import utils
import base64
import mimetypes


upload_image_endpoint = "https://bsky.social/xrpc/com.atproto.repo.uploadBlob"



def upload_blob(file_path):
        # Image-based posts require uploading the image first. This function uploads
        # the file and returns the image metadata to be used in the subsequent post.

        # Get Bluesky session
        session = utils.get_bluesky_session()
    
         # Determine the correct MIME type based on the file extension
        mime_type, _ = mimetypes.guess_type(file_path)
        
        with open(file_path, 'rb') as image_file:
            img_bytes = image_file.read()

        # Max file size is 1 MB.
        if len(img_bytes) > 1000000:
            print(f"Error: Image file size is too large. 1,000,000 bytes maximum, got: {len(img_bytes)}")
            return None

        headers = {
            'Content-Type': mime_type,
            'Authorization': f"Bearer {session['accessJwt']}"
        }
        image_response = requests.post(upload_image_endpoint, headers=headers, data=img_bytes)
        
        print(f"Image upload response code: {image_response.status_code}")  
        print(f"Image upload response content: {image_response.content}")  
        
        image_response.raise_for_status()

        return image_response.json()




def create_simple_post(text, date):
    # Create a simple post with text only.

    # Get Bluesky session
    session = utils.get_bluesky_session()


    # Required fields that each post must include
    post = {
        "$type": "app.bsky.feed.post",
        "text": text,
        "createdAt": date
    }

    resp = requests.post(
        "https://bsky.social/xrpc/com.atproto.repo.createRecord",
        headers={"Authorization": "Bearer " + session["accessJwt"]},
        json={
            "repo": session["did"],
            "collection": "app.bsky.feed.post",
            "record": post,
        },
    )

    resp.raise_for_status()



def create_simple_post_reply(text, date, rooturi, rootcid, parenturi, parentcid):
    # Create a reply to a post.
    # For threaded replies, you'll need the uri and cid of both the parent (the original post)
    # and the child post that is the parent of this post.

    # Get Bluesky session
    session = utils.get_bluesky_session()

    # Post data for replies
    post = {
        "$type": "app.bsky.feed.post",
        "text": text,
        "createdAt": date,
        "reply": {
            "root": {
            "uri": rooturi,
            "cid": rootcid
            },
            "parent": {
            "uri": parenturi,
            "cid": parentcid
            }
        }
    }

    resp = requests.post(
        "https://bsky.social/xrpc/com.atproto.repo.createRecord",
        headers={"Authorization": "Bearer " + session["accessJwt"]},
        json={
            "repo": session["did"],
            "collection": "app.bsky.feed.post",
            "record": post,
        },
    )

    #print(json.dumps(resp.json(), indent=2))
    resp.raise_for_status()




def create_post_with_image(text, date, file_path):
    # Create a post containing an image.
    
    # Get Bluesky session
    session = utils.get_bluesky_session()

    blob = upload_blob(file_path)
        

    # Required fields that each post must include
    post = {
        "$type": "app.bsky.feed.post",
        "text": text,
        "createdAt": date
    }

    # The image(s) to upload. Note that you can have multiple images, but this
    # functionality only uses one.
    post["embed"] = {
        "$type": "app.bsky.embed.images",
        "images": [
            {
                'image': blob['blob'],  # Use the blob metadata as-is
                'alt': 'An image attached to the post'
            }
        ]
    }   


    thejson={
            "repo": session["did"],
            "collection": "app.bsky.feed.post",
            "record": post,
        }
    
    

    resp = requests.post(
        "https://bsky.social/xrpc/com.atproto.repo.createRecord",
        headers={"Authorization": "Bearer " + session["accessJwt"]},
        json=thejson,
    )
    
    resp.raise_for_status()






if __name__ == "__main__":

    # Use current date/time
    #date = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    
    # ... or, specify a date/time, using the following format: "2024-08-07T05:31:12.156888Z"
    date = "2023-07-04T01:00:00.000000Z"


    # Text for the post
    text = "Hello, Bluesky!"

   
    # Create the post
    create_simple_post(text, date)



    # Create a post with an image
    # create_post_with_image(text, date, "c:\\image.jpg")



    # Create a reply to a post. Lookup the uri and cid of the parent image first.
    # Note for threads, these will be separate (original post and replies)
    # uri = "xyz"	
    # cid = "abc"
    # create_simple_post_reply(text, date, cid, uri, cid, uri)

 
