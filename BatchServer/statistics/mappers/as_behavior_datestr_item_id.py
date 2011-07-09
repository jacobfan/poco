import sys
import datetime


def getDateStr(timestamp):
    try:
        dt = datetime.datetime.fromtimestamp(timestamp)
    except:
        raise Exception("Can't parse timestamp: %r" % (timestamp,))
    date_str = dt.strftime("%Y-%m-%d")
    return date_str


for line in sys.stdin:
    timestamp, filled_user_id, behavior, tjbid, item_id = line.strip().split('\t')
    print '\t'.join((behavior, getDateStr(float(timestamp)), item_id))
