import os
from google.cloud import bigquery
import pandas as pd
import hashlib
from datetime import datetime


class NeonBQConnectorBuilder:
    def __init__(self):
        self.is_cache_enabled = True
        self.cache_path = './query_cache/'
        self.cache_res = 24 * 60  # 1 day
        self.use_sa = False
        self.auth_sa_path = None
        self.create_cache_path = True

    def enable_cache(self):
        self.is_cache_enabled = True
        if self.cache_path is None:
            self.cache_path = "./query_cache/"
        return self

    def disable_cache(self):
        self.is_cache_enabled = False
        self.cache_path = None
        return self

    def set_cache_path(self, cache_path, create_if_missing=True):
        if not cache_path[:2] != './' and not cache_path[0] != '/':
            cache_path = './' + cache_path
        self.cache_path = cache_path
        self.create_cache_path = create_if_missing
        return self

    def set_cache_expire_daily(self):
        self.cache_res = 24 * 60
        return self

    def set_cache_expire_weekly(self):
        self.cache_res = 7 * 24 * 60
        return self

    def set_cache_expire_monthly(self):
        self.cache_res = 30 * 7 * 24 * 60
        return self

    def set_custom_cache_expire_interval(self, interval):
        self.cache_res = int(interval)
        return self

    def authenticate_with_gcloud(self):
        self.use_sa = False
        self.auth_sa_path = None

        return self

    def authenticate_with_service_account(self, path):
        if not os.path.exists(path):
            raise RuntimeError("Specified service account path does not exist")

        self.use_sa = True
        self.auth_sa_path = path

        return self

# to keep track of cache, use a file containing an object. each key is the hash of the query. the key points to an
# object containing teh configuration for the cache file (expire, format, ...)


def get_data_from_queryfile(path, force_refresh=False):
    query = ""

    if not os.path.exists(path):
        raise RuntimeError("La path specificata non esiste")

    if os.stat(path).st_size == 0:
        raise RuntimeError("Il file specificato è vuoto")

    with open(path, 'r') as fp:
        query = fp.read()

    return get_data(query, force_refresh)


def get_data(query, force_refresh=False):
    if query is None or query == "":
        raise RuntimeError("La query passata come input è vuota!")

    base_name = hashlib.sha256(query.encode('utf-8')).hexdigest()
    today = datetime.now().strftime("%Y%m%d")
    file_name = os.path.abspath(f"./query_cache/{today}_{base_name}.parquet.gzip")

    data = None

    if not os.path.exists(os.path.abspath("./query_cache/")):
        os.mkdir("./query_cache/")

    if os.path.isfile(file_name):
        if force_refresh or os.stat(file_name).st_size == 0:
            os.path.remove(file_name)
        else:
            data = pd.read_parquet(path=file_name)
            if data.empty:
                os.path.remove(file_name)
                data = None

    if data is None:
        bqclient = bigquery.Client()

        dryrun_config = bigquery.QueryJobConfig(
            dry_run=True, use_query_cache=False
        )

        dryrun_job = bqclient.query(
            query, dryrun_config
        )

        gbyte_to_bill = dryrun_job.total_bytes_processed / (1024 * 1024 * 1024)

        print(f"Questa query processerà {gbyte_to_bill:.2f}GB ({(gbyte_to_bill / 1024) * 6.25:.2f}$).")
        print("Procedere? [Y/n]")

        ans = None
        while ans not in ["Y", "N"]:
            ans = input()
            ans = ans.strip().upper()[0]

        if ans == "N":
            raise RuntimeError("Interrotto dall'utente")

        query_job = bqclient.query(query)
        data = query_job.result().to_dataframe()
        data.to_parquet(path=file_name, compression="gzip", index=False)

    return data
