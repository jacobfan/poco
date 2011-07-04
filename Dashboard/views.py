import sys
sys.path.insert(0, "../")
import hashlib
import datetime
from django.http import HttpResponse
from django.shortcuts import render_to_response
from django.shortcuts import redirect
from django.template import RequestContext
import pymongo
from common.utils import getSiteDBCollection

import simplejson as json

from ApiServer.mongo_client import MongoClient

import settings


def getConnection():
    return pymongo.Connection(settings.mongodb_host)

mongo_client = MongoClient(getConnection())


def getSiteStatistics(site_id, days=7):
    c_statistics = getSiteDBCollection(getConnection(), site_id, "statistics")
    today_date = datetime.date.today()
    result = []
    for day_delta in range(0, days):
        the_date = today_date - datetime.timedelta(days=day_delta)
        the_date_str = the_date.strftime("%Y-%m-%d")
        row = c_statistics.find_one({"date": the_date_str})
        if row is None:
            row = {"date": the_date_str, "is_available": False}
        else:
            row["is_available"] = True
	    row["PV_UV"] = "%.2f" % (float(row["PV_V"]) / float(row["UV_V"]))
        result.append(row)
    return result


def login_required(callable):
    def method(request):
        if not request.session.has_key("user_name"):
            return redirect("/login")
        return callable(request)
    return method


@login_required
def index(request):
    user_name = request.session["user_name"]
    connection = getConnection()
    c_users = connection["tjb-db"]["users"]
    c_sites = connection["tjb-db"]["sites"]
    user = c_users.find_one({"user_name": user_name})
    sites = [c_sites.find_one({"site_id": site_id}) for site_id in user["sites"]]
    for site in sites:
        site["items_count"] = getItemsAndCount(connection, site["site_id"])[1]
        site["statistics"] = getSiteStatistics(site["site_id"])
    return render_to_response("index.html", {"sites": sites, "user_name": user_name})


def getItemsAndCount(connection, site_id):
    c_items = getSiteDBCollection(connection, site_id, "items")
    items_cur = c_items.find({"available": True})
    items_count = items_cur.count()
    return items_cur, items_count


@login_required
def site_items_list(request):
    site_id = request.GET["site_id"]
    connection = getConnection()
    site = connection["tjb-db"]["sites"].find_one({"site_id": site_id})
    items_cur, items_count = getItemsAndCount(connection, site_id)
    return render_to_response("site_items_list.html", 
            {"site": site, "items_count": items_count,
             "user_name": request.session["user_name"],
             "items": items_cur})


import cgi
import urlparse
from common.utils import APIAccess
api_access = APIAccess(settings.api_server_name, settings.api_server_port)


def _getItemIdFromRedirectUrl(redirect_url):
    parsed_qs = cgi.parse_qs(urlparse.urlparse(redirect_url).query)
    item_id = parsed_qs["item_id"][0]
    return item_id


def _getTopnByAPI(site, path, item_id, amount):
    result = api_access("/%s" % path,
               {"api_key": site["api_key"],
                "item_id": item_id,
                "user_id": "null",
                "amount": amount,
                "not_log_action": "yes",
                "include_item_info": "yes"}
               )
    if result["code"] == 0:
        topn = result["topn"]
        for topn_item in topn:
            topn_item["item_link"] = "/show_item?site_id=%s&item_id=%s" % (site["site_id"], _getItemIdFromRedirectUrl(topn_item["item_link"]))
        return topn

def _getUltimatelyBought(site, item_id, amount):
    topn = _getTopnByAPI(site, "getUltimatelyBought", item_id, 15)
    for topn_item in topn:
        topn_item["score"] = "%.1f%%" % (topn_item["score"] * 100)
    return topn

@login_required
def show_item(request):
    site_id = request.GET["site_id"]
    item_id = request.GET["item_id"]
    connection = getConnection()
    site = connection["tjb-db"]["sites"].find_one({"site_id": site_id})
    c_items = getSiteDBCollection(connection, site_id, "items")
    item_in_db = c_items.find_one({"item_id": item_id})
    return render_to_response("show_item.html",
        {"item": item_in_db, "user_name": request.session["user_name"], 
         "getAlsoViewed": _getTopnByAPI(site, "getAlsoViewed", item_id, 15),
         "getAlsoBought": _getTopnByAPI(site, "getAlsoBought", item_id, 15),
         "getBoughtTogether": _getTopnByAPI(site, "getBoughtTogether", item_id, 15),
         "getUltimatelyBought": _getUltimatelyBought(site, item_id, 15)
         })


def loadCategoryGroupsSrc(site_id):
    connection = getConnection()
    site = connection["tjb-db"]["sites"].find_one({"site_id": site_id})
    return site.get("category_groups_src", "")

from common.utils import updateCategoryGroups
@login_required
def update_category_groups(request):
    if request.method == "GET":
        site_id = request.GET["site_id"]
        category_groups_src = loadCategoryGroupsSrc(site_id)
        return render_to_response("update_category_groups.html",
                {"site_id": site_id, "category_groups_src": category_groups_src})


#from django.views.decorators.csrf import csrf_exempt

@login_required
#@csrf_exempt
def ajax_update_category_groups(request):
    if request.method == "GET":
        site_id = request.GET["site_id"]
        category_groups_src = request.GET["category_groups_src"]
        connection = getConnection()
        is_succ, msg = updateCategoryGroups(connection, site_id, category_groups_src)
        result = {"is_succ": is_succ, "msg": msg}
        return HttpResponse(json.dumps(result))


# Authentication System
def logout(request):
    del request.session["user_name"]
    return redirect("/")


def login(request):
    if request.method == "GET":
        msg = request.GET.get("msg", None)
        return render_to_response("login.html", {"msg": msg}, 
                  context_instance=RequestContext(request))
    else:
        conn = getConnection()
        users = conn["tjb-db"]["users"]
        user_in_db = users.find_one({"user_name": request.POST["name"]})
        login_succ = False
        if user_in_db is not None:
            login_succ = user_in_db["hashed_password"] == hashlib.sha256(request.POST["password"] + user_in_db["salt"]).hexdigest()

        if login_succ:
            request.session["user_name"] = request.POST["name"]
            return redirect("/")
        else:
            return redirect("/login?msg=login_failed")


import copy
def _getCurrentUser(request):
    conn = getConnection()
    if request.session.has_key("user_name"):
        return conn["tjb-db"]["users"].find_one({"user_name": request.session["user_name"]})
    else:
        return None

import os.path
def serve_jquery(request):
    file_path = os.path.join(os.path.dirname(__file__), 'static/jquery-1.6.1.min.js')
    return HttpResponse(open(file_path, "r").read())
