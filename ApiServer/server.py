#!/usr/bin/env python

import sys
sys.path.insert(0, "../")
import tornado.ioloop
import tornado.web
import simplejson as json
import copy
import re
import time
import os
import os.path
import signal
import uuid
import settings
import getopt


import mongo_client


# jquery serialize()  http://api.jquery.com/serialize/
# http://stackoverflow.com/questions/5784400/un-jquery-param-in-server-side-python-gae
# http://www.tsangpo.net/2010/04/24/unserialize-param-in-python.html

# TODO: referer; 
# TODO: when to reload site ids.

class LogWriter:
    def __init__(self):
        self.count = 0
        self.last_timestamp = None

    def writeEntry(self, site_id, content):
        timestamp = time.time()
        if timestamp <> self.last_timestamp:
            self.count = 0
        else:
            self.count += 1
        self.last_timestamp = timestamp
        timestamp_plus_count = "%r+%s" % (timestamp, self.count)
        content["timestamp"] = timestamp_plus_count
        if settings.print_raw_log:
            print "RAW LOG: site_id: %s, %s" % (site_id, content)
        mongo_client.writeLogToMongo(site_id, content)


def extractArguments(request):
    result = {}
    for key in request.arguments.keys():
        result[key] = request.arguments[key][0]
    return result

class ArgumentProcessor:
    def __init__(self, definitions):
        self.definitions = definitions

    def processArgs(self, args):
        err_msg = None
        result = {}
        for argument_name, is_required in self.definitions:
            if not args.has_key(argument_name):
                if is_required:
                    err_msg = "%s is required." % argument_name
                else:
                    result[argument_name] = None
            else:
                result[argument_name] = args[argument_name]

        return err_msg, result

class ArgumentError(Exception):
    pass


# TODO: how to update cookie expires
class APIHandler(tornado.web.RequestHandler):
    def get(self):
        args = extractArguments(self.request)
        site_id = args.get("site_id", None)
        callback = args.get("callback", None)

        site_ids = mongo_client.getSiteIds()
        if site_id not in site_ids:
            response = {'code': 2}
        else:
            del args["site_id"]
            if callback is not None:
                del args["callback"]
            try:
                response = self.process(site_id, args)
            except ArgumentError as e:
                response = {"code": 1, "err_msg": e.message}
        response_json = json.dumps(response)
        if callback != None:
            response_text = "%s(%s)" % (callback, response_json)
        else:
            response_text = response_json
        self.write(response_text)

    def process(self, site_id, args):
        pass


class TjbIdEnabledHandlerMixin:
    def prepare(self):
        tornado.web.RequestHandler.prepare(self)
        self.tuijianbaoid = self.get_cookie("tuijianbaoid")
        if not self.tuijianbaoid:
            self.tuijianbaoid = str(uuid.uuid4())
            self.set_cookie("tuijianbaoid", self.tuijianbaoid, expires_days=109500)


class SingleRequestHandler(TjbIdEnabledHandlerMixin, APIHandler):
    processor_class = None
    def process(self, site_id, args):
        processor = self.processor_class()
        err_msg, args = processor.processArgs(args)
        if err_msg:
            return {"code": 1, "err_msg": err_msg}
        else:
            args["tuijianbaoid"] = self.tuijianbaoid
            return processor.process(site_id, args)



class ActionProcessor:
    action_name = None
    def logAction(self, site_id, action_content, tjb_id_required=True):
        assert self.action_name != None
        if tjb_id_required:
            assert action_content.has_key("tjbid")
        action_content["behavior"] = self.action_name
        logWriter.writeEntry(site_id,
            action_content)

    def processArgs(self, args):
        return self.ap.processArgs(args)

    def process(self, site_id, args):
        pass


class ViewItemProcessor(ActionProcessor):
    action_name = "V"
    ap = ArgumentProcessor(
         (("item_id", True),
         ("user_id", True) # if no user_id, pass in "null"
        )
    )

    def process(self, site_id, args):
        self.logAction(site_id,
                {"user_id": args["user_id"],
                 "tjbid": args["tuijianbaoid"],
                 "item_id": args["item_id"]})
        return {"code": 0}






class ViewItemHandler(SingleRequestHandler):
    processor_class = ViewItemProcessor


# addFavorite LogFormat: timestamp,AF,user_id,tuijianbaoid,item_id


class AddFavoriteProcessor(ActionProcessor):
    action_name = "AF"
    ap = ArgumentProcessor(
        (
         ("item_id", True),
         ("user_id", True),
        )
    )
    def process(self, site_id, args):
        self.logAction(site_id,
                        {"user_id": args["user_id"], 
                         "tjbid": args["tuijianbaoid"], 
                         "item_id": args["item_id"]})
        return {"code": 0}

class AddFavoriteHandler(SingleRequestHandler):
    processor_class = AddFavoriteProcessor


class RemoveFavoriteProcessor(ActionProcessor):
    action_name = "RF"
    ap = ArgumentProcessor(
         (("item_id", True),
         ("user_id", True),
        )
    )
    def process(self, site_id, args):
        self.logAction(site_id, 
                        {"user_id": args["user_id"], 
                         "tjbid": args["tuijianbaoid"], 
                         "item_id": args["item_id"]})
        return {"code": 0}


class RemoveFavoriteHandler(SingleRequestHandler):
    processor_class = RemoveFavoriteProcessor


class RateItemProcessor(ActionProcessor):
    action_name = "RI"
    ap = ArgumentProcessor(
         (("item_id", True),
         ("score", True),
         ("user_id", True),
        )
    )
    def process(self, site_id, args):
        self.logAction(site_id, 
                        {"user_id": args["user_id"], 
                         "tjbid": args["tuijianbaoid"], 
                         "item_id": args["item_id"],
                         "score": args["score"]})
        return {"code": 0}


class RateItemHandler(SingleRequestHandler):
    processor_class = RateItemProcessor


# FIXME: check user_id, the user_id can't be null.


class AddShopCartProcessor(ActionProcessor):
    action_name = "ASC"
    ap = ArgumentProcessor(
        (
         ("user_id", True),
         ("item_id", True),
        )
    )
    def process(self, site_id, args):
        self.logAction(site_id,
                        {"user_id": args["user_id"], 
                         "tjbid": args["tuijianbaoid"], 
                         "item_id": args["item_id"]})
        return {"code": 0}

class AddShopCartHandler(SingleRequestHandler):
    processor_class = AddShopCartProcessor



class RemoveShopCartProcessor(ActionProcessor):
    action_name = "RSC"
    ap = ArgumentProcessor(
        (
         ("user_id", True),
         ("item_id", True),
        )
    )
    def process(self, site_id, args):
        self.logAction(site_id,
                        {"user_id": args["user_id"], 
                         "tjbid": args["tuijianbaoid"], 
                         "item_id": args["item_id"]})
        return {"code": 0}

class RemoveShopCartHandler(SingleRequestHandler):
    processor_class = RemoveShopCartProcessor


class PlaceOrderProcessor(ActionProcessor):
    action_name = "PLO"
    ap = ArgumentProcessor(
        (
         ("user_id", True),
         # order_content Format: item_id,price,amount|item_id,price,amount
         ("order_content", True), 
        )
    )

    def _convertOrderContent(self, order_content):
        result = []
        for row in order_content.split("|"):
            item_id, price, amount = row.split(",")
            result.append({"item_id": item_id, "price": price,
                           "amount": amount})
        return result

    def process(self, site_id, args):
        self.logAction(site_id,
                       {"user_id": args["user_id"], 
                        "tjbid": args["tuijianbaoid"],
                        "order_content": self._convertOrderContent(args["order_content"])})
        return {"code": 0}

class PlaceOrderHandler(SingleRequestHandler):
    processor_class = PlaceOrderProcessor


class UpdateItemProcessor(ActionProcessor):
    action_name = "UItem"
    ap = ArgumentProcessor(
         (("item_id", True),
         ("item_link", True),
         ("item_name", True),
         ("description", False),
         ("image_link", False),
         ("price", False),
         ("categories", False)
        )
    )

    def process(self, site_id, args):
        err_msg, args = self.ap.processArgs(args)
        if err_msg:
            return {"code": 1, "err_msg": err_msg}
        else:
            if args["description"] is None:
                del args["description"]
            if args["image_link"] is None:
                del args["image_link"]
            if args["price"] is None:
                del args["price"]
            if args["categories"] is None:
                del args["categories"]
            mongo_client.updateItem(site_id, args)
            return {"code": 0}



# FIXME: update/remove item should be called in a secure way.
class UpdateItemHandler(APIHandler):
    processor = UpdateItemProcessor()

    def process(self, site_id, args):
        return self.processor.process(site_id, args)


class RemoveItemProcessor(ActionProcessor):
    action_name = "RItem"
    ap = ArgumentProcessor(
         [("item_id", True)]
        )

    def process(self, site_id, args):
        err_msg, args = self.ap.processArgs(args)
        if err_msg:
            return {"code": 1, "err_msg": err_msg}
        else:
            mongo_client.removeItem(site_id, args["item_id"])
            return {"code": 0}



class RemoveItemHandler(APIHandler):
    processor = RemoveItemProcessor()

    def process(self, site_id, args):
        return self.processor.process(site_id, args)


def generateReqId():
    return str(uuid.uuid4())


class BaseSimilarityProcessor(ActionProcessor):
    similarity_type = None

    ap = ArgumentProcessor(
         (("user_id", True),
         ("item_id", True),
         ("include_item_info", False), # no, not include; yes, include
         ("amount", True),
        )
    )

    def logRecommendationRequest(self, args, site_id, req_id):
        self.logAction(site_id,
                        {"req_id": req_id,
                         "user_id": args["user_id"], 
                         "tjbid": args["tuijianbaoid"], 
                         "item_id": args["item_id"],
                         "amount": args["amount"]})

    def process(self, site_id, args):
        topn = mongo_client.recommend_viewed_also_view(site_id, self.similarity_type, args["item_id"], 
                        int(args["amount"]))
        include_item_info = args["include_item_info"] == "yes" or args["include_item_info"] is None
        req_id = generateReqId()
        topn = mongo_client.convertTopNFormat(site_id, req_id, topn, include_item_info)
        #topn = mongo_client.getCachedVAV(args["site_id"], args["item_id"]) 
        #                #,int(args["amount"]))
        self.logRecommendationRequest(args, site_id, req_id)
        return {"code": 0, "topn": topn, "req_id": req_id}


class RecommendViewedAlsoViewProcessor(BaseSimilarityProcessor):
    action_name = "RecVAV"
    similarity_type = "V"

class RecommendViewedAlsoViewHandler(SingleRequestHandler):
    processor_class = RecommendViewedAlsoViewProcessor


class BoughtAlsoBuyProcessor(BaseSimilarityProcessor):
    action_name = "RecBAB"
    similarity_type = "PLO"


class BoughtAlsoBuyHandler(SingleRequestHandler):
    processor_class = BoughtAlsoBuyProcessor


class BoughtTogetherProcessor(BaseSimilarityProcessor):
    action_name = "RecBTG"
    similarity_type = "BuyTogether"

class BoughtTogetherHandler(SingleRequestHandler):
    processor_class = BoughtTogetherProcessor


class ViewedUltimatelyBuyProcessor(ActionProcessor):
    action_name = "RecVUB"
    ap = ArgumentProcessor(
         (("user_id", True),
         ("item_id", True),
         ("include_item_info", False), # no, not include; yes, include
         ("amount", True),
        )
    )

    def logRecommendationRequest(self, args, site_id, req_id):
        self.logAction(site_id,
                        {"req_id": req_id,
                         "user_id": args["user_id"], 
                         "tjbid": args["tuijianbaoid"], 
                         "item_id": args["item_id"],
                         "amount": args["amount"]})

    def process(self, site_id, args):
        topn = mongo_client.recommend_viewed_ultimately_buy(site_id, args["item_id"], int(args["amount"]))
        include_item_info = args["include_item_info"] == "yes" or args["include_item_info"] is None
        req_id = generateReqId()
        topn = mongo_client.convertTopNFormat(site_id, req_id, topn, include_item_info)
        for topn_item in topn:
            topn_item["percentage"] = int(round(topn_item["score"] * 100))
        self.logRecommendationRequest(args, site_id, req_id)
        return {"code": 0, "topn": topn, "req_id": req_id}


class ViewedUltimatelyBuyHandler(SingleRequestHandler):
    processor_class = ViewedUltimatelyBuyProcessor


class RecommendBasedOnBrowsingHistoryProcessor(ActionProcessor):
    action_name = "RecBOBH"
    ap = ArgumentProcessor(
    (
     ("user_id", True),
     ("browsing_history", False),
     ("include_item_info", False), # no, not include; yes, include
     ("amount", True),
    ))

    def logRecommendationRequest(self, args, site_id, req_id):
        browsing_history = args["browsing_history"].split(",")
        self.logAction(site_id,
                        {"req_id": req_id,
                         "user_id": args["user_id"], 
                         "tjbid": args["tuijianbaoid"], 
                         "amount": args["amount"],
                         "browsing_history": browsing_history})

    def process(self, site_id, args):
        browsing_history = args["browsing_history"]
        if browsing_history == None:
            browsing_history = []
        else:
            browsing_history = browsing_history.split(",")
        try:
            amount = int(args["amount"])
        except ValueError:
            return {"code": 1}
        include_item_info = args["include_item_info"] == "yes" or args["include_item_info"] is None
        topn = mongo_client.recommend_based_on_browsing_history(site_id, "V", browsing_history, amount)
        req_id = generateReqId()
        topn = mongo_client.convertTopNFormat(site_id, req_id, topn, include_item_info)
        self.logRecommendationRequest(args, site_id, req_id)
        return {"code": 0, "topn": topn, "req_id": req_id}





class RecommendBasedOnBrowsingHistoryHandler(SingleRequestHandler):
    processor_class = RecommendBasedOnBrowsingHistoryProcessor


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write('{"version": "Tuijianbao v1.0"}')


class RecommendedItemRedirectHandler(TjbIdEnabledHandlerMixin, tornado.web.RequestHandler):
    def get(self):
        url = self.request.arguments.get("url", [None])[0]
        site_id = self.request.arguments.get("site_id", [None])[0]
        req_id = self.request.arguments.get("req_id", [None])[0]
        item_id = self.request.arguments.get("item_id", [None])[0]
        if url is None or site_id not in mongo_client.getSiteIds():
            # FIXME
            self.redirect("")
            return
        else:
            log_content = {"behavior": "ClickRec", "url": url, 
                           "req_id": req_id, "item_id": item_id, "site_id": site_id,
                           "tuijianbaoid": self.tuijianbaoid}
            logWriter.writeEntry(site_id, log_content)
            self.redirect(url)
            return


ACTION_NAME2PROCESSOR_CLASS = {}
def fillActionName2ProcessorClass():
    _g = globals()
    global ACTION_NAME2PROCESSOR_CLASS
    processor_classes = []
    for key in _g.keys():
        if type(_g[key]) == type(ActionProcessor) and issubclass(_g[key], ActionProcessor):
            processor_classes.append(_g[key])
    for processor_class in processor_classes:
        ACTION_NAME2PROCESSOR_CLASS[processor_class.action_name] = processor_class
fillActionName2ProcessorClass()


def getAbbrName2RelatedInfo(abbr_map):
    result = {}
    _g = globals()
    for request_type in abbr_map.keys():
        for attr_abbr in abbr_map[request_type].keys():
            processor_class = ACTION_NAME2PROCESSOR_CLASS[abbr_map[request_type]["action_name"]]
            result[request_type + attr_abbr] = (processor_class,
                                                request_type,
                                                abbr_map[request_type][attr_abbr])
    return result


import packed_request
ABBR_NAME2RELATED_INFO = getAbbrName2RelatedInfo(packed_request._abbr_map)

MASK2ACTION_NAME = packed_request.MASK2ACTION_NAME

ACTION_NAME2REQUEST_TYPE = packed_request.ACTION_NAME2REQUEST_TYPE


# 1. use the masks
# 2. shared params
# 3. overriding
class PackedRequestHandler(TjbIdEnabledHandlerMixin, APIHandler):
    def extractRequests(self, args):
        global ABBR_NAME2RELATED_INFO
        args = copy.copy(args)
        result = {}
        shared_params = {}
        remain_args = {}

        if not args.has_key("-"):
            raise ArgumentError("missing '-' argument")

        try:
            mask_set = int(args["-"], 16)
            del args["-"]
        except ValueError:
            raise ArgumentError("invalid '-' argument")

        for key in args.keys():
            if key.startswith("_"):
                shared_params[key[1:]] = args[key]
            else:
                remain_args[key] = args[key]

        for mask in MASK2ACTION_NAME.keys():
            if mask & mask_set != 0:
                action_name = MASK2ACTION_NAME[mask]
                request_type = ACTION_NAME2REQUEST_TYPE[action_name]
                processor_class = ACTION_NAME2PROCESSOR_CLASS[action_name]
                result[(processor_class, request_type)] = copy.copy(shared_params)

        for key in remain_args.keys():
            _processor, request_type, attr_name = ABBR_NAME2RELATED_INFO.get(key, (None, None, None))
            if _processor is None:
                raise ArgumentError("invalid param:%s" % key)
            else:
                if not result.has_key((_processor, request_type)):
                    raise ArgumentError("argument %s not covered by mask_set." % key)
                result[(_processor, request_type)][attr_name] = args[key]

        return result

    def redirectRequest(self, site_id, processor_class, request_args):
        request_args["site_id"] = site_id
        processor = processor_class()
        err_msg, processed_args = processor.processArgs(request_args)
        if err_msg:
            return {"code": 1, "err_msg": err_msg}
        else:
            processed_args["tuijianbaoid"] = self.tuijianbaoid
            result = processor.process(site_id, processed_args)
            return result

    def process(self, site_id, args):
        requests = self.extractRequests(args)
        response = {"code": 0, "responses": {}}
        for processor_class, request_type in requests.keys():
            request_args = requests[(processor_class, request_type)]
            response["responses"][request_type] = \
                self.redirectRequest(site_id, processor_class, request_args)
        return response


handlers = [
    (r"/", MainHandler),
    (r"/1.0/viewItem", ViewItemHandler),
    (r"/1.0/addFavorite", AddFavoriteHandler),
    (r"/1.0/removeFavorite", RemoveFavoriteHandler),
    (r"/1.0/rateItem", RateItemHandler),
    (r"/1.0/removeItem", RemoveItemHandler),
    (r"/1.0/updateItem", UpdateItemHandler),
    (r"/1.0/addShopCart", AddShopCartHandler),
    (r"/1.0/removeShopCart", RemoveShopCartHandler),
    (r"/1.0/placeOrder", PlaceOrderHandler),
    (r"/1.0/viewedAlsoView", RecommendViewedAlsoViewHandler),
    (r"/1.0/basedOnBrowsingHistory", RecommendBasedOnBrowsingHistoryHandler),
    (r"/1.0/boughtAlsoBuy", BoughtAlsoBuyHandler),
    (r"/1.0/boughtTogether", BoughtTogetherHandler),
    (r"/1.0/viewedUltimatelyBuy", ViewedUltimatelyBuyHandler),
    # TODO: and based on cart content
    (r"/1.0/packedRequest", PackedRequestHandler),
    (r"/1.0/redirect", RecommendedItemRedirectHandler)
    ]

def main():
    opts, _ = getopt.getopt(sys.argv[1:], 'p:', ['port='])
    port = settings.server_port
    for o, p in opts:
        if o in ['-p', '--port']:
	    try:
	        port = int(p)
	    except ValueError:
                print "port should be integer"
    global logWriter
    logWriter = LogWriter()
    application = tornado.web.Application(handlers)
    application.listen(port, settings.server_name)
    print "Listen at %s:%s" % (settings.server_name, port)
    tornado.ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    main()
