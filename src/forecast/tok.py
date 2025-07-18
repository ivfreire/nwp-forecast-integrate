
import numpy as np
import pandas as pd
import xarray as xr

from src.forecast.base import Base

# =========================================================================== #

DIMS_MAPPING = {
    'Times': 'times',
}

FIELDS_MAPPING = {
    'wind_speed_at_100m_agl': 'ws100m',
    'wind_speed_at_10m_agl': 'ws10m',
    'wind_from_direction_at_100m_agl': 'wd100m',
    'wind_from_direction_at_10m_agl': 'wd10m',
    'air_temperature_at_2m_agl': 't2m',
    'air_pressure_at_surface': 'ps',
    'relative_humidity_at_2m_agl': 'rh2m',
    'cloud_area_fraction': 'cldafrac',
    'lwe_precipitation_sum': 'precip-total',
}

# --------------------------------------------------------------------------- #

class TOK(Base):

# --------------------------------------------------------------------------- #

    def __str__(self):
        return f'TOK({self.uri=})'

# --------------------------------------------------------------------------- #

    @staticmethod
    def preprocess_dataset(ds: xr.Dataset) -> xr.Dataset:
        ds = ds.rename(DIMS_MAPPING)
        
        ds = ds.rename_vars({
            key: value
            for key, value in FIELDS_MAPPING.items()
            if key in ds.data_vars
        })

        ds.coords['latitude'] = ds.XLAT.mean(axis=0).mean(axis=1)
        ds.coords['longitude'] = ds.XLONG.mean(axis=0).mean(axis=0)

        return ds

# --------------------------------------------------------------------------- #

    @staticmethod
    def extract_point(ds: xr.Dataset, lat: float, lon: float) -> pd.DataFrame:
        point_ds = ds.isel(
            south_north=np.abs(ds.latitude - lat).argmin().values,
            west_east=np.abs(ds.longitude - lon).argmin().values
        )

        point_df = point_ds.to_dataframe().reset_index()
        point_df = point_df.melt(
            id_vars=['model_id', 'times'],
            value_vars=ds.data_vars,
            var_name='variable_id',
        )

        point_df['times'] = pd.to_datetime(point_df['times'].astype(str),
                                           format='%Y-%m-%d_%H:%M:%S')

        return point_df

# =========================================================================== #