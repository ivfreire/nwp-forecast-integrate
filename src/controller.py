
import re

import pandas as pd

from src.gcp.connector import engine
from src import model

# =========================================================================== #

pattern = re.compile(
    r"downloads/"
    r"(?P<model>[^/]+)/"
    r"(?P<round_dt>\d{8}T\d{2})/"
    r"(?P<variable>[^_]+)_"
    r"(?P<submodel>[^_]+)_"
    r"(?P<region>[^_]+)_"
    r"(?P<timestamp>\d{8}T\d{2})_"
    r"(?P<integration>[^.]+)\."
    r"(?P<extension>\w+)"
)

# =========================================================================== #

class Controller:

# --------------------------------------------------------------------------- #

    @staticmethod
    def parse_attributes(data: dict):
        match = pattern.match(data['name'])

        if not match:
            raise ValueError("Invalid file name format")
        
        attributes = match.groupdict() | {'sink': data.get('sink', 'bigquery')}
        attributes['uri'] = '/gcs/{}/{}'.format(data['bucket'], data['name'])

        return attributes

# --------------------------------------------------------------------------- #

    @staticmethod
    def fetch_points():
        return pd.read_sql(
            sql='''
                SELECT
                    t.name AS tech,
                    s.group_id,
                    s.subpark_id,
                    s.lat,
                    s.lon
                FROM subpark s
                JOIN technology_customer_relation tcr ON
                    tcr.group_id = s.group_id
                    AND tcr.tech_id = s.tech_id
                JOIN technology t ON
                    t.tech_id = s.tech_id
                WHERE
                    t.name = 'wind'
                    AND tcr.active
            ''',
            con=engine
        )

# --------------------------------------------------------------------------- #

    @staticmethod
    def process(data: dict):
        attributes = Controller.parse_attributes(data)

        PROCESSOR = model.MODEL_PROCESSOR_MAPPING[attributes['model']]

        points_df = Controller.fetch_points()

        processor = PROCESSOR(**attributes)
        processor.ingest_points(points_df)

        print(f'Processed {len(points_df)} points for {attributes["model"]} model.')

# =========================================================================== #