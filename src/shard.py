
import os
import re

import pandas as pd

from src.gcp.connector import engine
from src import forecast

# =========================================================================== #

ALLOWED_MODELS = os.getenv('ALLOWED_MODELS').split(',')
ALLOWED_VARIABLES = os.getenv('ALLOWED_VARIABLES').split(',')

# --------------------------------------------------------------------------- #

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

# --------------------------------------------------------------------------- #

class Shard:

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
    def is_shard_valid(attributes: dict):
        '''Checks if the shard is valid for processing. Should return true.'''
        allowed_models = set(ALLOWED_MODELS) & set(forecast.MODEL_PROCESSOR_MAPPING.keys())
        
        return (
            (attributes['model'] in list(allowed_models))
            and (attributes['variable'] in ALLOWED_VARIABLES)
        )

# --------------------------------------------------------------------------- #

    @staticmethod
    def fetch_points():
        return pd.read_csv(
            'gs://tok-power-config/sample-point-partition-forecast.csv',
            dtype={'point_id': str, 'lat': float, 'lon': float}
        )

# --------------------------------------------------------------------------- #

    @staticmethod
    def process(data: dict):
        try:
            attributes = Shard.parse_attributes(data)

            if not Shard.is_shard_valid(attributes):
                print(f'Skipping invalid shard {data["name"]}.')
                return 0

            PROCESSOR = forecast.MODEL_PROCESSOR_MAPPING[attributes['model']]

            points_df = Shard.fetch_points()

            processor = PROCESSOR(**attributes)
            processor.ingest_points(points_df)

            print(f'Processed {len(points_df)} points for {attributes["model"]} model.')
            return 0

        except Exception as ex:
            print(f'Error processing shard {data["name"]}: {ex}')
            return 1

# =========================================================================== #