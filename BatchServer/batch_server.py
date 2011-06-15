import logging
import sys
sys.path.append("../")
import time
import pymongo
import uuid
import os
import os.path
import settings
from ApiServer import mongo_client
from common.utils import getSiteDBCollection

# TODO: use hamake?

sys.path.insert(0, "../")

logging.basicConfig(format="%(asctime)s|%(levelname)s|%(name)s|%(message)s",
                    level=logging.INFO,
                    datefmt="%Y-%m-%d %I:%M:%S")

logger = logging.getLogger("Batch Server")


class ShellExecutionError(Exception):
    pass

class BaseFlow:
    def __init__(self, name):
        self.name = name
        self.logger = logging.getLogger()
        self.jobs = []
        self.dependencies = []

    def dependOn(self, flow):
        self.parent = flow
        flow.dependencies.append(self)

    def __call__(self):
        writeFlowBegin(SITE_ID, self.__class__.__name__)
        if self.__class__.__name__ in DISABLEDFLOWS:
            self.logger.info("%s is in DISABLEDFLOWS, skipped." % self.__class__.__name__)
            writeFlowEnd(SITE_ID, self.__class__.__name__, is_successful=True, is_skipped=True)
            return True
        else:
            for job_callable in self.jobs:
                if not self._execJob(job_callable):
                    writeFlowEnd(SITE_ID, self.__class__.__name__, is_successful=False, is_skipped=False,
                                    err_msg="SOME_JOBS_FAILED")
                    CALC_SUCC = False
                    return False

            global CALC_SUCC
            # execute downlines
            for dependency in self.dependencies:
                CALC_SUCC = CALC_SUCC and dependency()
            writeFlowEnd(SITE_ID, self.__class__.__name__, is_successful=True, is_skipped=False)
            return True

    def _exec_shell(self, command):
        logger.info("Execute %s" % command)
        ret_code = os.system(command)
        if ret_code != 0:
            raise ShellExecutionError("Shell Execution Failed, ret_code=%s" % ret_code)

    def _execJob(self, callable):
        try:
            logger.info("About to Start Job: %s" % callable)
            callable()
            logger.info("Job: %s succeeds." % callable)
            return True
        except:
            logger.critical("An Exception happened while running Job: %s" % callable,
                exc_info=True)
            # TODO: send message (email, sms)
            # TODO: record exception info.
            writeFailedJob(SITE_ID, self.__class__.__name__, callable.__name__)
            return False


class PreprocessingFlow(BaseFlow):
    def __init__(self):
        BaseFlow.__init__(self, "preprocessing")
        self.work_dir = settings.work_dir
        self.jobs += [self.do_backfill,
                      self.do_reverse_reversed_backfilled_raw_logs]

    def do_backfill(self):
        from preprocessing import backfiller
        last_ts = None # FIXME: load correct last_ts from somewhere
        connection = pymongo.Connection(settings.mongodb_host)
        bf = backfiller.BackFiller(connection, SITE_ID, last_ts, 
                    os.path.join(settings.work_dir, "reversed_backfilled_raw_logs"))
        last_ts = bf.start() # FIXME: save last_ts somewhere 

    def do_reverse_reversed_backfilled_raw_logs(self):
        input_path  = os.path.join(settings.work_dir, "reversed_backfilled_raw_logs")
        output_path = os.path.join(settings.work_dir, "backfilled_raw_logs")
        self._exec_shell("%s <%s >%s" % (settings.tac_command, input_path, output_path))


class BaseSimilarityCalcFlow(BaseFlow):
    def __init__(self, type):
        BaseFlow.__init__(self, "similarities-calc:%s" % type)
        self.type = type
        self.work_dir = os.path.join(settings.work_dir, "item_similarities_%s" % type)
        if not os.path.isdir(self.work_dir):
            os.mkdir(self.work_dir)
        self.jobs += self.getExtractUserItemMatrixJobs() + [self.do_sort_user_item_matrix,
                      self.do_emit_cooccurances,
                      self.do_sort_cooccurances,
                      self.do_count_cooccurances,
                      self.do_format_item_similarities,
                      self.do_make_item_similarities_bi_directional,
                      self.do_sort_item_similarities_bi_directional,
                      self.do_extract_top_n,
                      self.do_upload_item_similarities_result]


    def do_sort_user_item_matrix(self):
        input_path  = os.path.join(self.work_dir, "user_item_matrix")
        output_path = os.path.join(self.work_dir, "user_item_matrix_sorted")
        self._exec_shell("sort %s > %s" % (input_path, output_path))


    def do_emit_cooccurances(self):
        from similarity_calculation.amazon.emit_cooccurances import emit_cooccurances
        input_path  = os.path.join(self.work_dir, "user_item_matrix_sorted")
        output_path = os.path.join(self.work_dir, "cooccurances_not_sorted")
        emit_cooccurances(input_path, output_path)


    def do_sort_cooccurances(self):
        input_path  = os.path.join(self.work_dir, "cooccurances_not_sorted")
        output_path = os.path.join(self.work_dir, "cooccurances_sorted")
        self._exec_shell("sort %s > %s" % (input_path, output_path))


    def do_count_cooccurances(self):
        input_path  = os.path.join(self.work_dir, "cooccurances_sorted")
        output_path = os.path.join(self.work_dir, "item_similarities_raw")
        self._exec_shell("uniq -c %s > %s" % (input_path, output_path))


    def do_format_item_similarities(self):
        from similarity_calculation.amazon.format_item_similarities import format_item_similarities
        input_path  = os.path.join(self.work_dir, "item_similarities_raw")
        output_path = os.path.join(self.work_dir, "item_similarities_formatted")
        format_item_similarities(input_path, output_path)


    def do_make_item_similarities_bi_directional(self):
        from similarity_calculation.make_similarities_bidirectional import make_similarities_bidirectional
        input_path  = os.path.join(self.work_dir, "item_similarities_formatted")
        output_path = os.path.join(self.work_dir, "item_similarities_bi_directional")
        make_similarities_bidirectional(input_path, output_path)


    def do_sort_item_similarities_bi_directional(self):
        input_path  = os.path.join(self.work_dir, "item_similarities_bi_directional")
        output_path = os.path.join(self.work_dir, "item_similarities_bi_directional_sorted")
        self._exec_shell("sort %s > %s" % (input_path, output_path))


    def do_extract_top_n(self):
        from similarity_calculation.extract_top_n import extract_top_n
        input_path  = os.path.join(self.work_dir, "item_similarities_bi_directional_sorted")
        output_path = os.path.join(self.work_dir, "item_similarities_top_n")
        n = 20
        extract_top_n(input_path, output_path, n)


    def do_upload_item_similarities_result(self):
        from common.utils import UploadItemSimilarities
        import pymongo
        input_path = os.path.join(self.work_dir, "item_similarities_top_n")
        connection = pymongo.Connection(settings.mongodb_host)
        uis = UploadItemSimilarities(connection, SITE_ID, self.type)
        uis(input_path)


class VSimiliarityCalcFlow(BaseSimilarityCalcFlow):
    def __init__(self):
        BaseSimilarityCalcFlow.__init__(self, "V")

    def getExtractUserItemMatrixJobs(self):
        return [self.do_extract_user_item_matrix,
                self.do_de_duplicate_user_item_matrix]

    def do_extract_user_item_matrix(self):
        from preprocessing.extract_user_item_matrix import v_extract_user_item_matrix
        input_path  = os.path.join(self.parent.work_dir, "backfilled_raw_logs")
        output_path = os.path.join(self.work_dir, "user_item_matrix_maybe_dup")
        v_extract_user_item_matrix(input_path, output_path)

    def do_de_duplicate_user_item_matrix(self):
        input_path  = os.path.join(self.work_dir, "user_item_matrix_maybe_dup")
        output_path = os.path.join(self.work_dir, "user_item_matrix")
        self._exec_shell("sort < %s | uniq > %s" % (input_path, output_path))


class PLOSimilarityCalcFlow(BaseSimilarityCalcFlow):
    def __init__(self):
        BaseSimilarityCalcFlow.__init__(self, "PLO")

    def getExtractUserItemMatrixJobs(self):
        return [self.do_extract_user_item_matrix,
                self.do_de_duplicate_user_item_matrix]

    def do_extract_user_item_matrix(self):
        from preprocessing.extract_user_item_matrix import plo_extract_user_item_matrix
        input_path  = os.path.join(self.parent.work_dir, "backfilled_raw_logs")
        output_path = os.path.join(self.work_dir, "user_item_matrix_maybe_dup")
        plo_extract_user_item_matrix(input_path, output_path)

    def do_de_duplicate_user_item_matrix(self):
        input_path  = os.path.join(self.work_dir, "user_item_matrix_maybe_dup")
        output_path = os.path.join(self.work_dir, "user_item_matrix")
        self._exec_shell("sort < %s | uniq > %s" % (input_path, output_path))


class BuyTogetherSimilarityFlow(BaseSimilarityCalcFlow):
    def __init__(self):
        BaseSimilarityCalcFlow.__init__(self, "BuyTogether")

    def getExtractUserItemMatrixJobs(self):
        return [self.do_extract_user_item_matrix,
                self.do_de_duplicate_user_item_matrix]

    def do_extract_user_item_matrix(self):
        from preprocessing.extract_user_item_matrix import buytogether_extract_user_item_matrix
        input_path  = os.path.join(self.parent.work_dir, "backfilled_raw_logs")
        output_path = os.path.join(self.work_dir, "user_item_matrix_maybe_dup")
        buytogether_extract_user_item_matrix(input_path, output_path)

    def do_de_duplicate_user_item_matrix(self):
        input_path  = os.path.join(self.work_dir, "user_item_matrix_maybe_dup")
        output_path = os.path.join(self.work_dir, "user_item_matrix")
        self._exec_shell("sort < %s | uniq > %s" % (input_path, output_path))


class ViewedUltimatelyBuyFlow(BaseFlow):
    def __init__(self):
        BaseFlow.__init__(self, "preprocessing")
        self.work_dir = os.path.join(settings.work_dir, "viewed_ultimately_buy")
        if not os.path.isdir(self.work_dir):
            os.mkdir(self.work_dir)
        self.jobs += [self.do_extract_user_view_buy_logs,
                      self.do_sort_user_view_buy_logs,
                      self.do_pair_view_buy,
                      self.count_pairs,
                      self.count_item_view,
                      self.upload_viewed_ultimately_buy]

    def do_extract_user_view_buy_logs(self):
        from viewed_ultimately_buy.extract_user_view_buy_logs import extract_user_view_buy_logs
        input_path  = os.path.join(self.parent.work_dir, "backfilled_raw_logs")
        output_path = os.path.join(self.work_dir, "user_view_buy_logs")
        extract_user_view_buy_logs(input_path, output_path)

    def do_sort_user_view_buy_logs(self):
        input_path  = os.path.join(self.work_dir, "user_view_buy_logs")
        output_path = os.path.join(self.work_dir, "user_view_buy_logs_sorted")
        self._exec_shell("sort <%s >%s" % (input_path, output_path))

    def do_pair_view_buy(self):
        from viewed_ultimately_buy.pair_view_buy import pair_view_buy
        input_path  = os.path.join(self.work_dir, "user_view_buy_logs_sorted")
        output_path = os.path.join(self.work_dir, "view_buy_pairs")
        pair_view_buy(input_path, output_path)

    def count_pairs(self):
        input_path  = os.path.join(self.work_dir, "view_buy_pairs")
        output_path = os.path.join(self.work_dir, "view_buy_pairs_counted")
        self._exec_shell("sort <%s | uniq -c >%s" % (input_path, output_path))

    def count_item_view(self):
        #FIXME a hack
        input_path = os.path.join(settings.work_dir, "item_similarities_V", "user_item_matrix")
        output_path = os.path.join(self.work_dir, "item_view_times")
        self._exec_shell("cut -d , -f 2 <%s | sort | uniq -c >%s" % (input_path, output_path))

    def upload_viewed_ultimately_buy(self):
        from viewed_ultimately_buy.upload_viewed_ultimately_buy import upload_viewed_ultimately_buy
        item_view_times_path = os.path.join(self.work_dir, "item_view_times")
        view_buy_pairs_counted_path = os.path.join(self.work_dir, "view_buy_pairs_counted")
        connection = pymongo.Connection(settings.mongodb_host)
        upload_viewed_ultimately_buy(connection, SITE_ID, item_view_times_path, view_buy_pairs_counted_path)

class BeginFlow(BaseFlow):
    def __init__(self):
        BaseFlow.__init__(self, "Root")
        self.jobs += [self.begin]

    def begin(self):
        self.logger.info("Start Work on %s", SITE_ID)

class FinishFlow(BaseFlow):
    def __init__(self):
        BaseFlow.__init__(self, "Root")
        self.jobs += [self.finish]

    def finish(self):
        #TODO: set last finished work flag in database
        logger.info("Finish Work on %s", SITE_ID)

# TODO: removed items' similarities should also be removed.

begin_flow = BeginFlow()

preprocessing_flow = PreprocessingFlow()
preprocessing_flow.dependOn(begin_flow)

v_similarity_calc_flow = VSimiliarityCalcFlow()
v_similarity_calc_flow.dependOn(preprocessing_flow)

plo_similarity_calc_flow = PLOSimilarityCalcFlow()
plo_similarity_calc_flow.dependOn(preprocessing_flow)


buy_together_similarity_flow = BuyTogetherSimilarityFlow()
buy_together_similarity_flow.dependOn(preprocessing_flow)


viewed_ultimately_buy_flow = ViewedUltimatelyBuyFlow()
viewed_ultimately_buy_flow.dependOn(preprocessing_flow)


#finish_flow = FinishFlow()
#finish_flow.dependOn(similarity_calc_flow)

# TODO: use calculation_id in logs.


# CALCULATION_RECORD = {"id": "", "begin_timestamp": "", "end_timestamp": "", "is_successful": "",
#                        "flows": {
#                               "<flow_name>": {"begin_timestamp": "", "end_timestamp": "", "is_successful": <>, "failed_job_name": ""}
#                         }
#                      }

def createCalculationRecord(site_id):
    connection = pymongo.Connection(settings.mongodb_host)
    calculation_id = str(uuid.uuid4())
    record = {"calculation_id": calculation_id, "begin_timestamp": time.time(), "flows": {}}
    calculation_records = getSiteDBCollection(connection, site_id, "calculation_records")
    calculation_records.save(record)
    return calculation_id


def getCalculationRecord(site_id, calculation_id):
    connection = pymongo.Connection(settings.mongodb_host)
    calculation_records = getSiteDBCollection(connection, site_id, "calculation_records")
    return calculation_records.find_one({"calculation_id": calculation_id})


def updateCalculationRecord(site_id, record):
    connection = pymongo.Connection(settings.mongodb_host)
    calculation_records = getSiteDBCollection(connection, site_id, "calculation_records")
    calculation_records.save(record)


def writeFailedJob(site_id, flow_name, failed_job_name):
    record = getCalculationRecord(SITE_ID, CALCULATION_ID)
    flow_record = record["flows"][flow_name]
    flow_record["failed_job_name"] = failed_job_name
    updateCalculationRecord(SITE_ID, record)


def writeFlowBegin(site_id, flow_name):
    record = getCalculationRecord(SITE_ID, CALCULATION_ID)
    record["flows"][flow_name] = {"begin_timestamp": time.time()}
    updateCalculationRecord(SITE_ID, record)


def writeFlowEnd(site_id, flow_name, is_successful, is_skipped, err_msg = None):
    record = getCalculationRecord(SITE_ID, CALCULATION_ID)
    flow_record = record["flows"][flow_name] 
    flow_record["end_timestamp"] = time.time()
    flow_record["is_successful"] = is_successful
    flow_record["is_skipped"] = is_skipped
    if not is_successful:
        flow_record["err_msg"] = err_msg
    updateCalculationRecord(SITE_ID, record)


def writeCalculationEnd(site_id, is_successful, err_msg = None):
    record = getCalculationRecord(SITE_ID, CALCULATION_ID)
    record["end_timestamp"] = time.time()
    record["is_successful"] = is_successful
    if not is_successful:
        record["err_msg"] = err_msg
    updateCalculationRecord(SITE_ID, record)


# format:
#     {"event": "BEGIN_CALC", "timestamp": <timestamp>, "calculation_id": <calculation_id>}
#     {"event": "BEGIN_FLOW", "timestamp": <timestamp>, "flow_name": "", "calculation_id": <calculation_id>}
#     {"event": "BEGIN_JOB",  "timestamp": <timestamp>, "job_name": "", "calculation_id": <calculation_id>}
#     {"event": "END_JOB",    "timestamp": <timestamp>, "job_name": "", "is_successful": <true or false>, "calculation_id": <calculation_id>}
#     {"event": "END_FLOW", "timestamp": <timestamp>, "flow_name": "", "calculation_id": <calculation_id>, "is_successful": <true or false>, , "is_skipped": <True or False>}
#     {"event": "END_CALC", "timestamp": <timestamp>, "calculation_id": <calculation_id>, "is_successful": <true or false>, "reason": ""}
#     "timestamp" and calculation_id are automatically added by this function.
def writeCalculationLog(site_id, content):
    connection = pymongo.Connection(settings.mongodb_host)
    calculation_logs = getSiteDBCollection(connection, site_id, "calculation_logs")
    content["timestamp"] = time.time()
    content["calculation_id"] = CALCULATION_ID
    calculation_logs.insert(content)


if __name__ == "__main__":
    while True:
        for site in mongo_client.loadSites():
            now = time.time()
            if site.get("last_update_ts") is None \
                or now - site.get("last_update_ts") > site["calc_interval"]:
                SITE_ID = site["site_id"]
                DISABLEDFLOWS = site.get("disabledFlows", [])
                CALCULATION_ID = createCalculationRecord(SITE_ID)
                CALC_SUCC = True
                try:
                    try:
                        begin_flow()
                        writeCalculationEnd(SITE_ID, CALC_SUCC, err_msg = "SOME_FLOWS_FAILED")
                    except:
                        logger.critical("Unexpected Exception:", exc_info=True)
                        writeCalculationEnd(SITE_ID, False, "UNEXPECTED_EXCEPTION")
                finally:
                    logger.info("====================================")
                #FIXME: save last_update_ts
        sleep_seconds = 10
        logger.info("Go to sleep for %s seconds." % sleep_seconds)
        time.sleep(sleep_seconds)
