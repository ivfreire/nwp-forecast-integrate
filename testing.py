
from src.controller import Controller

names = [
    # 'downloads/ECENS/20250717T00/u100m-inst_ECENS_bra-northeast_20250717T00_hourly.nc',
    # 'downloads/ECENS/20250717T00/v100m-inst_ECENS_bra-northeast_20250717T00_hourly.nc',
    # 'downloads/ECENS/20250717T00/ws100m-inst_ECENS_bra-northeast_20250717T00_hourly.nc',
    # 'downloads/ECENS45/20250716T00/u100m-inst_ECENS45_bra-northeast_20250716T00_6hourly.nc',
    # 'downloads/ECENS45/20250716T00/v100m-inst_ECENS45_bra-northeast_20250716T00_6hourly.nc',
    # 'downloads/ECENS45/20250716T00/ws100m-inst_ECENS45_bra-northeast_20250716T00_6hourly.nc',

    # 'downloads/GFS/20250717T00/u100m-inst_GFS_glob_20250717T00_20250717T00.grb',
    # 'downloads/GFS/20250717T00/u100m-inst_GFS_glob_20250717T00_20250717T06.grb',
    # 'downloads/GFS/20250717T00/v100m-inst_GFS_glob_20250717T00_20250717T00.grb',
    # 'downloads/GFS/20250717T00/v100m-inst_GFS_glob_20250717T00_20250717T06.grb',

    # 'downloads/TOK10d1/20250717T00/ws100m-inst_TOK10d1_bra_20250717T00_20250717T00.nc',
    # 'downloads/TOK10d1/20250717T00/ws100m-inst_TOK10d1_bra_20250717T00_20250717T06.nc',
    # 'downloads/TOK10d1/20250717T00/ws100m-inst_TOK10d1_bra_20250717T00_20250717T12.nc',
    # 'downloads/TOK10d1/20250717T00/wd100m-inst_TOK10d1_bra_20250717T00_20250717T00.nc',
    # 'downloads/TOK10d1/20250717T00/wd100m-inst_TOK10d1_bra_20250717T00_20250717T06.nc',
    # 'downloads/TOK10d1/20250717T00/wd100m-inst_TOK10d1_bra_20250717T00_20250717T12.nc',
    # 'downloads/TOK10d1/20250717T00/tmp2m-inst_TOK10d1_bra_20250717T00_20250717T00.nc',
    # 'downloads/TOK10d1/20250717T00/tmp2m-inst_TOK10d1_bra_20250717T00_20250717T06.nc',
    # 'downloads/TOK10d1/20250717T00/tmp2m-inst_TOK10d1_bra_20250717T00_20250717T12.nc',

    # 'downloads/TOK10d1/20250717T00/press-inst_TOK10d1_bra_20250717T00_20250727T06.nc',
    # 'downloads/TOK10d1/20250717T00/rh2m-inst_TOK10d1_bra_20250717T00_20250722T12.nc',
    # 'downloads/TOK10d1/20250717T00/cldafrac-inst_TOK10d1_bra_20250717T00_20250723T00.nc',
    # 'downloads/TOK10d1/20250717T00/precip-total_TOK10d1_bra_20250717T00_20250718T12.nc',
    # 'downloads/TOK10d1/20250717T00/ws10m-inst_TOK10d1_bra_20250717T00_20250726T18.nc',
    # 'downloads/TOK10d1/20250717T00/wd10m-inst_TOK10d1_bra_20250717T00_20250726T18.nc'
]

if __name__ == "__main__":
    for i, name in enumerate(names):
        print(f"Processing {i + 1}/{len(names)}: {name}")

        Controller.process({
            'bucket': 'tok_downloader',
            'sink': 'none',
            'name': name
        })