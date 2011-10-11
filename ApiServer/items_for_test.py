SITE_ID = "tester"

items = {
            "1": {"site_id": SITE_ID, "item_id": "1",
                  "item_link": "http://example.com/item?id=1",
                  "item_name": "Turk"},
            "3": {"site_id": SITE_ID, "item_id": "3", 
             "item_link": "http://example.com/item?id=3",
             "item_name": "Harry Potter I"},
            "2": {"site_id": SITE_ID, "item_id": "2", 
             "item_link": "http://example.com/item?id=2",
             "item_name": "Lord of Ring I"},
            "8": {"site_id": SITE_ID, "item_id": "8", 
             "item_link": "http://example.com/item?id=8",
             "item_name": "Best Books"},
            "11": {"site_id": SITE_ID, "item_id": "11", 
             "item_link": "http://example.com/item?id=11",
             "item_name": "Meditation"},
             "15": {"site_id": SITE_ID, "item_id": "15", 
             "item_link": "http://example.com/item?id=15",
             "item_name": "SaaS Book"},
             "17": {"site_id": SITE_ID, "item_id": "17", 
             "item_link": "http://example.com/item?id=17",
             "item_name": "Who am I"},
             "21": {"site_id": SITE_ID, "item_id": "21", 
             "item_link": "http://example.com/item?id=21",
             "item_name": "Kill A Bird"},
             "22": {"site_id": SITE_ID, "item_id": "22",
             "item_link": "http://example.com/item?id=22",
             "item_name": "The Rule of 22s"},
             "23": {"site_id": SITE_ID, "item_id": "23",
             "item_link": "http://example.com/item?id=23",
             "item_name": "The Rule of 22s"}, # This item name is intended to be the same as item 22
             "24": {"site_id": SITE_ID, "item_id": "24",
             "item_link": "http://example.com/item?id=24",
             "item_name": "The Rule of 22s"}, # This item name is intended to be the same as item 22
             "29": {"site_id": SITE_ID, "item_id": "29", 
             "item_link": "http://example.com/item?id=29",
             "item_name": "Soo..."},
             "30": {"site_id": SITE_ID, "item_id": "30",
             "item_link": "http://example.com/item?id=30",
             "item_name": "Not Recommended by Item 1"} # please DO NOT let item 1 recommend this one
        }


import pymongo
import settings


def getApiKey(site_id):
    connection = pymongo.Connection(settings.mongodb_host)
    return connection["tjb-db"]["sites"].find_one({"site_id": site_id})["api_key"]

for item in items.values():
    item["api_key"] = getApiKey(item["site_id"])
    del item["site_id"]
