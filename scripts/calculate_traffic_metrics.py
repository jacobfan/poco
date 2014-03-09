#!/usr/bin/env python
"""
This script is for initializing the traffic_metrics collection for sites based on items.
TODO: how to be updated with latest raw_logs?
"""
import sys
sys.path.insert(0, ".")
import datetime
import pymongo
from api import settings

from api.mongo_client import MongoClient
from common.utils import getSiteDBCollection

def getConnection():
    if(settings.replica_set):
        return pymongo.MongoReplicaSetClient(settings.mongodb_host, replicaSet=settings.replica_set)
    else:
        return pymongo.Connection(settings.mongodb_host)

mongo_client = MongoClient(getConnection())
mongo_client.reloadApiKey2SiteID()

if len(sys.argv) != 4:
    print "Usage: python calculate_traffic_metrics.py <site_id> <from_timestamp> <to_timestamp>"
    sys.exit(1)
else:
    site_id, from_timestamp_str, to_timestamp_str = sys.argv[1:]
    try:
        from_timestamp = datetime.datetime.strptime(from_timestamp_str, "%Y-%m-%d %H:%M:%S")
        to_timestamp = datetime.datetime.strptime(to_timestamp_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        print "Please invalid <from_timestamp> or <to_timestamp>"
        sys.exit(1)


print "START"
import time
t1 = time.time()
print "UPDATING site: ", site_id
c_raw_logs = getSiteDBCollection(mongo_client.connection, site_id, "raw_logs")
c_traffic_metrics = getSiteDBCollection(mongo_client.connection, site_id, "traffic_metrics")
c_traffic_metrics.ensure_index("item_id")
result_set = c_raw_logs.find({"created_on": {"$gte": from_timestamp, "$lte": to_timestamp}})
total = result_set.count()
count = 0
for raw_log in result_set:
    count += 1
    if (count % 2000) == 0:
        t2 = time.time()
        print "%s/%s, %s" % (count, total, count/(t2-t1))
    mongo_client.updateTrafficMetricsFromLog(site_id, raw_log)
t3 = time.time()
print "FINISHED"
