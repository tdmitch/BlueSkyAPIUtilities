import os
import requests
from dotenv import load_dotenv


def get_bluesky_session():

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