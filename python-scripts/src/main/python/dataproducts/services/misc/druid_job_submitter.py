"""
Script to submit druid report jobs.
"""

import os
import sys
import json
import requests
from dataproducts.resources.common import common_config
from dataproducts.util import kafka_utils

class DruidJobSubmitter:

    def __init__(self, report_search_base_url, auth_token, replace_list = """[{"key":"__store__","value":"azure"},{"key":"__container__","value":"reports"}]"""):
        self.report_search_base_url = report_search_base_url
        self.replace_list = json.loads(replace_list)
        self.auth_token = auth_token
        self.env = os.getenv("ENV", "dev")
        config = common_config.init()
        self.kafka_broker = os.getenv("KAFKA_BROKER_HOST", "localhost:9092")
        self.kafka_topic = config["kafka_job_queue"].format(self.env)
        print('config', self.report_search_base_url, self.replace_list)

    def get_active_jobs(self):
        url = "{}report/jobs".format(self.report_search_base_url)
        payload = """{"request": {"filters": {"status": ["ACTIVE"]}}}"""
        headers = {
            'content-type': "application/json; charset=utf-8",
            'cache-control': "no-cache",
            'Authorization': "Bearer " + self.auth_token
        }
        response = requests.request("POST", url, data=payload, headers=headers)
        print('Active report configurations fetched from the API')
        return response.json()['result']['reports']


    def interpolate_config(self, report_config):
        report_config_str = json.dumps(report_config)
        for item in self.replace_list:
            report_config_str = report_config_str.replace(item["key"], item["value"])
        print('String interpolation for the report config completed')
        return report_config_str


    def submit_job(self, report_config):
        report_config = json.loads(report_config)
        submit_config = json.loads("""{"model":"druid_reports", "config":{"search":{"type":"none"},"model":"org.ekstep.analytics.model.DruidQueryProcessingModel","output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"Druid Query Processor","deviceMapping":false,"modelParams": }}""")
        submit_config['config']['modelParams'] = report_config
        submit_config['config']['modelParams']['modelName'] = report_config['reportConfig']['id'] + "_job"
        kafka_utils.send(self.kafka_broker, self.kafka_topic, json.dumps(submit_config))
        print('Job submitted to the job manager with config - ', submit_config)
        return


    def init(self):
        print('Starting the job submitter...')
        reports = self.get_active_jobs()
        for report in reports:
            report_config = self.interpolate_config(report['config'])
            self.submit_job(report_config)
        print('Job submission completed...')
