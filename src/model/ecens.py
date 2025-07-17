
import pandas as pd
import xarray as xr

from src.model.model import Model

# =========================================================================== #

class ECENS(Model):

# --------------------------------------------------------------------------- #

    def __str__(self):
        return f'ECENS({self.uri=})'

# --------------------------------------------------------------------------- #

    @staticmethod
    def preprocess_dataset(ds: xr.Dataset) -> xr.Dataset:
        ecm0_ds = ds.sel(number=0)
        ecm0_ds.coords['model'] = 'ECENSm00'

        ecav_df = ds.mean(dim='number')
        ecav_df.coords['model'] = 'ECENSav'

        ds = xr.concat([ecm0_ds, ecav_df], dim='model')
        ds.coords['times'] = ds.time + ds.step

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

        return point_df

# =========================================================================== #