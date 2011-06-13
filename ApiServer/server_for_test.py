#!/usr/bin/env python
import sys
sys.path.insert(0, "../")
import server
import tornado.web


settings = server.settings
settings.server_port = 15588


# re-create database and upload similarities for test purpose.
import pymongo
from common import utils
connection = pymongo.Connection(settings.mongodb_host)
connection.drop_database(utils.getSiteDBName("tester"))
utils.UploadItemSimilarities(connection, "tester")("item_similarities_for_test")

if __name__ == "__main__":
    server.main()
