
import os
import configparser
import boto3
import pandas as pd 
from botocore.exceptions import ClientError
import logging

# logging.basicConfig(level=logging.DEBUG)

class S3MetaSync(object):

    def __init__(self, config_file=os.path.join('.s3metasync' ,'config.ini'), profile='user'):
        self.cfg = configparser.ConfigParser()
        if os.path.isfile(config_file):
            self.cfg.read(config_file)
            self.user_params = self.cfg[profile]
            save_fn = self.user_params.get('sync_file')
            dir_nm = os.path.dirname(config_file)
            self.save_path = os.path.abspath(os.path.relpath(os.path.join(dir_nm, save_fn)))
            self.bucket = self.user_params.get('bucket')

        self.client = boto3.client('s3')
        self.resource = boto3.resource('s3')
        
        if os.path.isfile(self.save_path):
            self.sync_sheet = pd.read_csv(self.save_path, index_col=0)
        else:
            self.sync_local_metadata()

        self.records_buffer = []


    def upload_file(self, file_name, meta_data, object_name=None):
        if object_name is None:
            object_name = file_name

        try:
            response = self.client.upload_file(
                file_name, 
                self.bucket, 
                object_name,
                ExtraArgs={'MetaData':meta_data}
            )
            record = meta_data.update({'object_key':object_name, 'object_bucket':self.bucket})
            # self.sync_sheet.append(record, ignore_index=True)
            self.records_buffer.append(record)
            return response
        except ClientError as e:
            logging.error(e)
            return False

    def sync_local_metadata(self):
        response = self.client.list_objects_v2(Bucket=self.bucket)
        objects = response['Contents']
        data = []
        for obj in objects:
            logging.log(logging.DEBUG, 'obj: {}'.format(obj))
            obj_data = {}
            obj_data['object_key'] = obj['Key']
            obj_data['object_bucket'] = self.bucket
            head_obj = self.client.head_object(
                Bucket=self.bucket,
                Key=obj['Key']
            )
            logging.debug('head: {}'.format(head_obj))
            logging.debug('head keys: {}'.format(list(head_obj.keys())))
            obj_data.update(head_obj['Metadata'])
            data.append(obj_data)

        temp = pd.DataFrame(data)
        self.sync_sheet = temp
        
        self.save()

    def flush_record_buffer(self):
        for _ in range(0, len(self.records_buffer)):
            record = self.records_buffer.pop()
            self.sync_sheet.append(record, ignore_index=True)
        
    def save(self):
        self.flush_record_buffer()
        self.sync_sheet.to_csv(self.save_path)










