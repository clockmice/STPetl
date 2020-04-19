import requests
import base64
import sys

OAUTH_TOKEN_URL = "https://accounts.spotify.com/api/token"
TRACKS_URL = "https://api.spotify.com/v1/tracks/"

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
        headers=headers
    )
    if response.status_code != 200:
        print(response.reason)
        exit()

    token_info = response.json()
    return token_info


def get_track_data(token, track_id):
    header = make_request_header(token)
    response = requests.get(
        TRACKS_URL + track_id,
        headers=header
    )
    if response.status_code != 200:
        print(response.reason)
        exit()

    return response.json()


if __name__ == "__main__":
    token_info = request_access_token(sys.argv[1], sys.argv[2])
    print(token_info)

    token = token_info['access_token']
    print(token)

    resp = get_track_data(token, '1Ku0J6YIKWOd6pZi4VlFLb')
    print(resp)