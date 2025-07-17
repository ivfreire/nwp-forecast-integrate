from src.model.ecens import ECENS
from src.model.gfs import GFS
from src.model.tok import TOK

# =========================================================================== #

MODEL_PROCESSOR_MAPPING = {
    'ECENS': ECENS,
    'ECENS45': ECENS,
    'GFS': GFS,
    'TOK10d1': TOK,
    'TOK10d2': TOK,
    'TOK30d1': TOK,
}
