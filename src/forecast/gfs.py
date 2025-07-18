
import pandas as pd
import xarray as xr

from src.forecast.base import Base

# =========================================================================== #

FIELDS_MAPPING = {
    'u100': 'u100m',
    'v100': 'v100m',
}

# --------------------------------------------------------------------------- #

class GFS(Base):

# --------------------------------------------------------------------------- #

    def __str__(self):
        return f'GFS({self.uri=})'

# --------------------------------------------------------------------------- #

    @staticmethod
    def preprocess_dataset(ds: xr.Dataset) -> xr.Dataset:
        ds = ds.expand_dims({"dummy": [0]})

        ds.coords['model'] = 'GFS'
        ds.coords['times'] = ds.time + ds.step

        ds = ds.rename_vars({
            key: value
            for key, value in FIELDS_MAPPING.items()
            if key in ds.data_vars
        })

        return ds

# --------------------------------------------------------------------------- #

    @staticmethod
    def extract_point(ds: xr.Dataset, lat: float, lon: float) -> pd.DataFrame:
        # Compensate for GFS longitude wrapping. 0 -> 360 deg
        point_ds = ds.sel(latitude=lat, longitude=lon + 180, method='nearest')
        
        point_df = point_ds.to_dataframe().reset_index()
        point_df = point_df.melt(
            id_vars=['model', 'times'],
            value_vars=ds.data_vars
        )

        return point_df

# =========================================================================== #