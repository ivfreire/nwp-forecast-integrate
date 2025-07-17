import os
import json

import sqlalchemy
from google.cloud import secretmanager
from google.cloud.sql.connector import Connector, IPTypes
from google.oauth2 import service_account

# =========================================================================== #

client = secretmanager.SecretManagerServiceClient()

# --------------------------------------------------------------------------- #

def creator(connector, instance: str, database: str, credentials: service_account.Credentials):
    return connector.connect(
        instance,
        "pg8000",
        user=credentials.service_account_email,
        password=None,
        db=database
    )

# --------------------------------------------------------------------------- #

database    = os.getenv('DATABASE_NAME')
instance    = os.getenv('DATABASE_INSTANCE')
secret_key  = os.getenv('DATABASE_CREDENTIALS')

secret_response = client.access_secret_version(request={'name': secret_key}) \
    .payload.data.decode('UTF-8')

credentials = service_account.Credentials.from_service_account_info(
    json.loads(secret_response),
    scopes=['https://www.googleapis.com/auth/sqlservice.admin']
)

connector = Connector(ip_type=IPTypes.PUBLIC, enable_iam_auth=True, 
                      credentials=credentials)

# --------------------------------------------------------------------------- #

engine = sqlalchemy.create_engine(
    "postgresql+pg8000://",
    creator=lambda: creator(connector, instance, database, credentials),
    pool_pre_ping=True, echo=False
)

# =========================================================================== #