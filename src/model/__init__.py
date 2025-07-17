from src.model.ecens import ECENS
from src.model.gfs import GFS
from src.model.tok import TOK
from src.model.tokmd import TOKMDcp

# =========================================================================== #

MODEL_PROCESSOR_MAPPING = {
    'GFS': GFS,
    'ECENS': ECENS,
    'ECENS45': ECENS,
    'TOK10d1': TOK,
    'TOK10d2': TOK,
    'TOK30d1': TOK,
    'TOKMDcp': TOKMDcp,
}
