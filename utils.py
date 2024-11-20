import os
import requests
from dotenv import load_dotenv
from atproto import Client


def get_bluesky_session():

    """
        Get a BlueSky session for write operations. This function returns a session key.
    """

    # Get environment variables
    load_dotenv()
    BLUESKY_HANDLE = os.getenv('BLUESKY_HANDLE')
    BLUESKY_APP_PASSWORD = os.getenv('BLUESKY_APP_PASSWORD')

    # For write operations, we'll need to create a session which contains the session-specific
    #  access token.
    resp = requests.post(
        "https://bsky.social/xrpc/com.atproto.server.createSession",
        json={"identifier": BLUESKY_HANDLE, "password": BLUESKY_APP_PASSWORD},
    )
    resp.raise_for_status()
    session = resp.json()

    # This is the session key we'll need for write operations
    # print(session["accessJwt"])

    return session

def get_client():

    """
        Get a BlueSky client for write operations. This function returns a client object.
    """
    client = Client()
    client.login(os.getenv('BLUESKY_HANDLE'), os.getenv('BLUESKY_APP_PASSWORD'))
    return client


def format_text(input):
    """
    This function takes a string input and returns a BlueSky formatted string with the
    text linked as indicated. Use the format below with the link and text specified
    in double curly braces.
    
    "Go to {{link='http://www.bsky.app' text='this site'}} for more info."
    
    """

    # Startindex of the link
    start = input.find(str.lower("{{link='"))
    
    # Start of the text, before any link
    startText = input[:start]

    # Text before the link
    tmp1 = input[(start + 8):]

    # Link text
    theLink = tmp1[:tmp1.find("'")]

    # Text to be linked
    linkedText = tmp1[(tmp1.find(str.lower(" text='")) + 7):(tmp1.find("'}}"))]

    # Endindex of the text to be linked
    end = start + len(linkedText)

    # Get any text after the link
    endText = input[input.find("'}}") + 3:]

    # The final formatted text to be returned
    theText = startText + linkedText + endText

    # output json
    json = {
        "text": theText,
        "facets": [
            {
                "index": {
                    "byteStart": start,
                    "byteEnd": end
                },
                "features": [{
                    "$type": "app.bsky.richtext.facet#link",
                    "uri": theLink
                }]
            }
        ]
    }


    return json