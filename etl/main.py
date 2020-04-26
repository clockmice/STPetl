from datetime import date

import base64
import jmespath
import requests
import sqlite3
import sys
import yaml


class TableData:
    def __init__(self, table_name, api_spec, col_names, col_types, col_values, upsert):
        self.table_name = table_name
        self.api_spec = api_spec
        self.col_names = col_names
        self.col_types = col_types
        self.col_values = col_values
        self.upsert = upsert

    def to_string(self):
        return f'Table name: {self.table_name},\nApi spec: {self.api_spec},\n' \
               f'Column names: {self.col_names},\nColumn types: {self.col_types}\n' \
               f'Column values: {self.col_values},\nUpsert: {self.upsert}'

    def generate_insert_stmt(self):
        cols = ', '.join(f'{col}' for col in self.col_names)
        vals = ', '.join(f':{col}' for col in self.col_names)
        return f'INSERT INTO "{self.table_name}" ({cols}) VALUES ({vals}) {self.upsert}'

    def generate_create_stmt(self):
        tuples = zip(self.col_names, self.col_types)
        s = []
        for t in tuples:
            s.append(' '.join(t))

        return f'CREATE TABLE IF NOT EXISTS {self.table_name} ({",".join(s)})'

    def add_values(self, values):
        self.col_values.append(values)


class Config:
    def __init__(self, batch_number, batch_size, db_connect, api_batch_size, oauth_token_url, tracks_url, input_path):
        self.batch_number = batch_number
        self.batch_size = batch_size
        self.db_connect = db_connect
        self.api_batch_size = api_batch_size
        self.oauth_token_url = oauth_token_url
        self.tracks_url = tracks_url
        self.input_path = input_path


def make_authorization_headers(client_id, client_secret):
    auth_header = base64.b64encode(
        (client_id + ':' + client_secret).encode('ascii')
    )
    return {'Authorization': f'Basic {auth_header.decode("ascii")}'}


def make_request_header(token):
    return {'Authorization': f'Bearer {token}'}


def request_access_token(config, client_id, client_secret):
    payload = {'grant_type': 'client_credentials'}

    headers = make_authorization_headers(
        client_id, client_secret
    )

    response = requests.post(
        config.oauth_token_url,
        data=payload,
        headers=headers,
        verify=True
    )
    if response.status_code != 200:
        print(response.reason)
        exit()

    token_info = response.json()
    return token_info


def request_api(config, session, track_ids):

    response = session.get(
        url=config.tracks_url + track_ids,
        verify=True
    )
    if response.status_code != 200:
        print(response.reason)
        exit()

    return response.json()


def initiate_db(config, tables):
    conn = sqlite3.connect(config.db_connect)
    c = conn.cursor()

    for t in tables:
        c.execute(t.generate_create_stmt())

    conn.commit()
    return conn


def get_tracks(config, session, track_ids):
    tracks = []
    for batch in track_ids:
       resp = request_api(config, session, ','.join(batch))
       tracks.extend(resp.get('tracks'))

    return tracks


def write_to_db(conn, tables):
    c = conn.cursor()

    for t in tables:
        c.executemany(t.generate_insert_stmt(), t.col_values)

    conn.commit()


def verify(conn, tables):
    # Print the table contents
    for t in tables:
        for row in conn.execute(f'select * from {t.table_name}'):
            print (row)


def create_tables(spec):
    tables = []
    for t in spec.get('tables'):
        col_names = []
        col_types = []
        api_spec = []
        for col in t.get('columns'):
            pair = col.split(':')
            col_names.append(pair[0])
            api_spec.append(pair[1])
            col_types.append(pair[2])
        upsert = t.get('upsert', '')
        table = TableData(t.get('table_name'), api_spec, col_names, col_types, [], upsert)
        tables.append(table)

    return tables


def get_track_ids(config):
    track_ids = []
    batch = []
    with open(config.input_path) as f:
        line_num = 1
        skip = (config.batch_number - 1) * config.batch_size
        for line in f:
            if skip < line_num <= (skip + config.batch_size):
                if len(batch) >= config.api_batch_size:
                    track_ids.append(batch)
                    batch = []
                batch.append(line.strip())
            line_num += 1
        track_ids.append(batch)

    return track_ids


if __name__ == '__main__':

    # Read config file
    spec = {}
    with open(sys.argv[1]) as f:
        spec = yaml.load(f, Loader=yaml.FullLoader)
    config = Config(spec.get('batch_number'), spec.get('batch_size'), spec.get('db_connect'),
                    spec.get('api_batch_size'),spec.get('oauth_token_url'),
                    spec.get('tracks_url'), spec.get('input_path'))

    tables = create_tables(spec)

    # Set up database
    conn = initiate_db(config, tables)

    # Get input track ids
    track_ids = get_track_ids(config)

    # Get token for API
    token_info = request_access_token(config, sys.argv[2], sys.argv[3])
    token = token_info.get('access_token')

    header = make_request_header(token)
    session = requests.Session()
    session.headers = header

    dt = date.today()

    # Get data from API
    tracks = get_tracks(config, session, track_ids)

    for track in tracks:
        for table in tables:
            track_data = []
            for spec in table.api_spec:
                if '{current_date}' in spec:
                    value = dt
                else:
                    value = jmespath.search(spec, track)

                track_data.append(value)

            table.add_values(track_data)

    write_to_db(conn, tables)
    verify(conn, tables)

    conn.close()
