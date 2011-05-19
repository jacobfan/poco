from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport
from thrift.protocol import TBinaryProtocol
from hbase.ttypes import Mutation
from hbase import Hbase

import md5

import simplejson as json

transport = TBufferedTransport(TSocket("localhost", 9090))
transport.open()
protocol = TBinaryProtocol.TBinaryProtocol(transport)

client = Hbase.Client(protocol)

ITEM_SIMILARITY_TABLE = "item_similarities"

def doHash(id):
    return md5.md5(id).hexdigest()


def updateItem(item):
    the_id = md5.md5(item["customer_id"] + ":" + item["item_id"]).hexdigest()
    item_json = json.dumps(item)
    client.mutateRow("items", the_id, [Mutation(column="p:content", value=item_json)])

def recommend_viewed_also_view(customer_id, item_id, amount):
    # FIXME: we ignore customer_id currently
    row = client.getRow(ITEM_SIMILARITY_TABLE, doHash(item_id))
    #item_id1 = row[0].columns["p:item_id1"].value
    #print "ITEM_ID1:", item_id1
    topn = json.loads(row[0].columns["p:mostSimilarItems"].value)[:amount]
    return topn
