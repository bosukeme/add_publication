from decouple import config as env_config

# S3 related parameters
s3_access_key=env_config("s3_access_key")
s3_secret_access_key=env_config("s3_secret_access_key")

