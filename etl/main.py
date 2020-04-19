import requests
import base64
import sys

OAUTH_TOKEN_URL = "https://accounts.spotify.com/api/token"
TRACKS_URL = "https://api.spotify.com/v1/tracks/?ids="

def make_authorization_headers(client_id, client_secret):
    auth_header = base64.b64encode(
        (client_id + ":" + client_secret).encode("ascii")
    )
    return {"Authorization": f"Basic {auth_header.decode('ascii')}"}


def make_request_header(token):
    return {"Authorization": f"Bearer {token}"}


def request_access_token(client_id, client_secret):
    """Gets client credentials access token """
    payload = {"grant_type": "client_credentials"}

    headers = make_authorization_headers(
        client_id, client_secret
    )

    response = requests.post(
        OAUTH_TOKEN_URL,
        data=payload,
        headers=headers,
        verify=True
    )
    if response.status_code != 200:
        print(response.reason)
        exit()

    token_info = response.json()
    return token_info


def get_tracks_data(session, track_ids):

    response = session.get(
        url=TRACKS_URL + track_ids,
        verify=True
    )
    if response.status_code != 200:
        print(response.reason)
        exit()

    return response.json()


if __name__ == "__main__":
    track_ids = []
    batch = []
    with open("resources/track_ids.txt") as f:
        for line in f:
            if len(batch) >= 50:
                track_ids.append(batch)
                batch = []

            batch.append(line.strip())
        track_ids.append(batch)

    token_info = request_access_token(sys.argv[1], sys.argv[2])
    print(token_info)

    token = token_info['access_token']
    print(token)

    header = make_request_header(token)
    session = requests.Session()
    session.headers = header

    for batch in track_ids:
        resp = get_tracks_data(session, ",".join(batch))
        for track in resp['tracks']:
            print(track['name'])