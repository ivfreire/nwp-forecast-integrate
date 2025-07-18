from src.forecast.ecens import ECENS
from src.forecast.gfs import GFS
from src.forecast.tok import TOK
from src.forecast.tokmd import TOKMDcp

# =========================================================================== #

MODEL_PROCESSOR_MAPPING = {
    'GFS':     GFS,
    'ECENS':   ECENS,
    'ECENS45': ECENS,
    'TOK10d1': TOK,
    'TOK10d2': TOK,
    'TOK30d1': TOK,
    'TOKMDcp': TOKMDcp,
}

# =========================================================================== #