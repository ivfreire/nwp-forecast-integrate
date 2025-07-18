
import numpy as np
import pandas as pd
import pandas_gbq as gbq
import xarray as xr

# =========================================================================== #

GLOBAL_REGIONS = ['glob']

TABLE_MAPPING = {
    'wind': 'tokwsv3.wind.sample_point_partition_forecast'
}

TABLE_COLUMNS = [
    'data_source', 'ingested_at', 'run_time', 'model_id', 'variable_id',
    'region_code', 'partition_time', 'point_id', 'latitude', 'longitude',
    'forecast'
]

class Base:

# --------------------------------------------------------------------------- #

    def __init__(self, uri: str = None, model: str = None, round_dt: str = None,
                 variable: str = None, submodel: str = None,
                 region: str = None, timestamp: str = None,
                 integration: str = None, extension: str = None,
                 sink: str = 'bigquery'):
        
        # Shard's attributes
        self.uri = uri
        self.model = model
        self.variable = variable
        self.submodel = submodel
        self.region = region
        self.extension = extension

        self.round_dt = pd.to_datetime(round_dt, format='%Y%m%dT%H')
        self.timestamp = pd.to_datetime(timestamp, format='%Y%m%dT%H', errors='coerce')
        self.partition_time = pd.to_datetime(integration, format='%Y%m%dT%H', errors='coerce')

        # Sink
        self.sink = sink

# --------------------------------------------------------------------------- #

    def open_dataset(self, filepath: str):
        print(f'Opening dataset from {filepath}.')
        
        ds = xr.open_dataset(filepath)
        ds.coords['model_id'] = self.model

        return ds

# --------------------------------------------------------------------------- #

    @staticmethod
    def filter_points_in(ds: xr.Dataset, points_df: pd.DataFrame):
        print('Filtering points within dataset bounds.')

        lat_min, lat_max = ds.latitude.min().values, ds.latitude.max().values
        lon_min, lon_max = ds.longitude.min().values, ds.longitude.max().values

        points_df = points_df[
            (points_df['lat'] >= lat_min) & (points_df['lat'] <= lat_max) &
            (points_df['lon'] >= lon_min) & (points_df['lon'] <= lon_max)
        ]

        if points_df.empty:
            raise ValueError("No points found within dataset bounds.")
        
        return points_df

# --------------------------------------------------------------------------- #

    def extract_points(self, ds: xr.Dataset, points_df: pd.DataFrame):
        return pd.concat([
            self.extract_point(ds, point.lat, point.lon) \
                .assign(
                    tech=point.tech,
                    point_id=point.point_id,
                )
            for point in points_df.itertuples()
        ])

# --------------------------------------------------------------------------- #

    @staticmethod
    def aggregate_points(result_df: pd.DataFrame):
        return result_df.rename(columns={'value': 'values'}) \
            .groupby(
                by=['tech', 'model_id', 'variable_id', 'point_id']
            ).apply(
                lambda df: pd.Series({
                    'forecast': df[['times', 'values']].to_dict(orient='records')
                })
            ).reset_index()

# --------------------------------------------------------------------------- #

    def assign_partition_info(self, result_df: pd.DataFrame):
        return result_df.assign(
            data_source=self.uri,
            ingested_at=pd.Timestamp.utcnow(),
            run_time=self.round_dt,
            region_code=self.region,
            partition_time=self.partition_time,
        )

# --------------------------------------------------------------------------- #

    @staticmethod
    def upload_points(points_df: pd.DataFrame):
        for tech, df in points_df.groupby('tech'):
            print(f'Uploading {len(df)} points for {tech} to BigQuery.')
            
            gbq.to_gbq(
                dataframe=df[TABLE_COLUMNS],
                destination_table=TABLE_MAPPING[tech],
                project_id='tokwsv3',
                if_exists='append',
            )

# --------------------------------------------------------------------------- #

    def ingest_points(self, points_df: pd.DataFrame):
        ds = self.open_dataset(self.uri)
        ds = self.preprocess_dataset(ds)

        if self.region not in GLOBAL_REGIONS:
            points_df = self.filter_points_in(ds, points_df)

        result_df = self.extract_points(ds, points_df)
        result_df = self.aggregate_points(result_df)
        
        result_df = result_df.merge(points_df, on=['tech', 'point_id']) \
            .rename(columns={'lat': 'latitude', 'lon': 'longitude'})

        result_df = self.assign_partition_info(result_df)

        if self.sink == 'bigquery':
            self.upload_points(result_df)
            
        elif self.sink == 'none':
            print("Sink is set to 'none', skipping upload.")
            print("Processed DataFrame:", result_df.head())

# =========================================================================== #