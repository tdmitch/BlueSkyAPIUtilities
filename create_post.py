import requests
import json
from datetime import datetime, timezone
import utils


def create_simple_post(text, date):
    
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
    print(json.dumps(resp.json(), indent=2))
    resp.raise_for_status()


if __name__ == "__main__":

    # Set the post time
    # Using a trailing "Z" is preferred over the "+00:00" format
    
    # Use current date/time
    # date = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    
    # ... or, specify a date/time, using the following format: "2024-08-07T05:31:12.156888Z"
    date = "2024-01-01T07:00:00.000000Z"

    text = "This is a test post from Python."

    # Create the post
    create_simple_post(text, date)
