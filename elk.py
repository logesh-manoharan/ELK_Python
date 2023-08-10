import MySQLdb as dbapi
# import mysql.connector
import os
# import sys
# import csv
# import time
import datetime
import traceback
import threading
import logging
import logging.config
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta

# import collections


# import json
logfile_name = "test_analysistime.log"
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    # filename="/mnt/future_splash/alm/alm.log",
                    filename=logfile_name,
                    filemode='a')


# METHOD 1: Hardcode zones:
class TotalFailures(object):
    def __init__(self, TerminationTimeout=120):
        self.TerminationTimeout = TerminationTimeout
        self.MaxProcesCount = 100
        self.ProcController = threading.BoundedSemaphore(self.MaxProcesCount)

    def push_total_failures(self):

        utc_now = datetime.now()
        yesterday = utc_now - timedelta(days=1)
        yesterday_date = yesterday.strftime("%Y-%m-%d")
        connection = dbapi.connect(host='96.118.140.163', db='soap', user='autop_238', passwd='auto@238Prod', ssl={})

        ncur = connection.cursor()

        # ELK OBJECT CREATED:
        es = Elasticsearch('tdk-data-asc-b-001.logging.comcast.net', http_auth=('splash_automatics', '0UeTBkfmot'),
                           verify_certs=False, port=9200, scheme="https")

        failures_qry = "select FROM_UNIXTIME(bd.StartedAt/1000,'%Y-%m-%d'),bd.CATEGORY_TYPE,count(bs.ID) from buildstatus bs join builddetails bd on bs.BUILD_DTLS_ID=bd.ID where bs.automated_status='FAIL' and bd.CATEGORY_TYPE in('RDKV','RDKB') and FROM_UNIXTIME(bd.StartedAt/1000,'%Y-%m-%d') >='" + yesterday_date + "' group by FROM_UNIXTIME(bd.StartedAt/1000,'%Y-%m-%d'),bd.CATEGORY_TYPE"

        ncur.execute(failures_qry)
        failure_values = ncur.fetchall()
        if len(failure_values) > 0:
            for failure_values_row in failure_values:
                failure_each_data = {}

                failure_each_data['timestamp'] = utc_now

                failure_each_data['deviceType'] = failure_values_row[1]
                failure_each_data['createdAt'] = failure_values_row[0]
                failure_each_data['count'] = failure_values_row[2]

                # DATA PUSH : Use New Index name and doc_type according to your requirements.
                res = es.index(index="build-total-failures", doc_type='build_total_failures', body=failure_each_data,
                               request_timeout=30);
                # analysis_data.append(analysis_each_data)
                print("Status: %s" % res['result'])
                print("failure_each_data", failure_each_data)


try:
    TotalFailures().push_total_failures()
except SystemExit:
    print("Exiting program...");