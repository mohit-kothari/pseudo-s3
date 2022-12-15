import os
from pydantic import BaseSettings


class Settings(BaseSettings):
    name = "pseudo-s3"
    data_root = "./buckets"
    date_fmt = "%Y-%m-%dT%H:%M:%S.000Z"
    model = os.getenv("MODEL", "models.disk_storage")
    valid_credentials = [
        {
            "access_key_id": os.getenv("AWS_ACCESS_KEY", "pseudoS3AccessKey"),
            "secret_key": os.getenv("AWS_SECRET_KEY", "pseudoS3SecretKey")
        }
    ]
    owner_id = "randomOwnerID"


settings = Settings()
