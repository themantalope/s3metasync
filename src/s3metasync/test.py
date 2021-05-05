from .metasync import S3MetaSync
import logging

logging.basicConfig(level=logging.DEBUG)

s3ms = S3MetaSync(config_file='test.ini')

s3ms.sync_local_metadata()