
import numpy as np
import pandas as pd
import xarray as xr

from src.model.model import Model

# =========================================================================== #

DIMS_MAPPING = {
    'time': 'times',
}

FIELDS_MAPPING = {
    'precip': 'precip-rate'
}

# --------------------------------------------------------------------------- #

class TOKMDcp(Model):

# --------------------------------------------------------------------------- #

    def __str__(self):
        return f'TOKMDcp({self.uri=})'

# --------------------------------------------------------------------------- #

    @staticmethod
    def preprocess_dataset(ds: xr.Dataset) -> xr.Dataset:
        ds = ds.rename(DIMS_MAPPING)
        
        ds = ds.rename_vars({
            key: value
            for key, value in FIELDS_MAPPING.items()
            if key in ds.data_vars
        })

        return ds

# --------------------------------------------------------------------------- #

    @staticmethod
    def extract_point(ds: xr.Dataset, lat: float, lon: float) -> pd.DataFrame:
        point_ds = ds.sel(latitude=lat, longitude=lon, method='nearest')

        point_df = point_ds.to_dataframe().reset_index()
        point_df = point_df.melt(
            id_vars=['model', 'times'],
            value_vars=ds.data_vars
        )

        # point_df['times'] = pd.to_datetime(point_df['times'].astype(str),
        #                                    format='%Y-%m-%d_%H:%M:%S')

        return point_df

# =========================================================================== #