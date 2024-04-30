import json
import os

from google.api_core.exceptions import BadRequest
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
        if self.cache_res < 1:
            return self.disable_cache()
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


class NeonBQConnector:
    def __init__(self, enable_cache, cache_path, cache_update, create_cache_path, sa_path):
        if enable_cache:
            self.cache_path = cache_path
            if create_cache_path:
                os.mkdir(self.cache_path)
            self.cache_update = cache_update

        if sa_path is not None and os.path.isfile(sa_path):
            self.client = bigquery.Client.from_service_account_json(sa_path)
        else:
            self.client = bigquery.Client()

        self.dryrun_config = bigquery.QueryJobConfig(
            dry_run=True, use_query_cache=False
        )

    def validate_query(self, query):
        retry = 0
        while retry < 3:
            try:
                retry = retry + 1
                dryrun_job = self.client.query(
                    query, self.dryrun_config
                )
                return dryrun_job.total_bytes_processed
            except BadRequest as e:
                data = json.loads(e.response.text)
                print(f"Attempt #{retry}: Error while parsing query: {data["error"]["message"]}")
                if retry == 3:
                    return -1

    def validate_query_from_file(self, query_path):
        query = ""

        if not os.path.exists(query_path):
            raise RuntimeError("La path specificata non esiste")

        if os.stat(query_path).st_size == 0:
            raise RuntimeError("Il file specificato è vuoto")

        with open(query_path, 'r') as fp:
            query = fp.read()

        return self.validate_query(query)

    def run(self, query):
        if query is None or query == "":
            raise RuntimeError("La query passata come input è vuota!")

        #TODO: change the following 3 lines
        base_name = hashlib.sha256(query.encode('utf-8')).hexdigest()
        today = datetime.now().strftime("%Y%m%d")
        file_name = os.path.abspath(f"{self.cache_path}/{today}_{base_name}.parquet.gzip")

        data = None

        if os.path.isfile(file_name):
            if os.stat(file_name).st_size == 0:
                os.path.remove(file_name)
            else:
                data = pd.read_parquet(path=file_name)
                if data.empty:
                    os.path.remove(file_name)
                    data = None

        if data is None:
            byte_to_bill = self.validate_query(query)

            if byte_to_bill == -1:
                raise RuntimeError("Query validation failed! Please fix the reported problems and run again!")

            #TODO: add option to run without asking
            byte_to_bill = byte_to_bill / (1024 * 1024 * 1024)

            print(f"Questa query processerà {byte_to_bill:.2f}GB ({(byte_to_bill / 1024) * 6.25:.2f}$).")
            print("Procedere? [Y/n]")

            ans = None
            while ans not in ["Y", "N"]:
                ans = input()
                ans = ans.strip().upper()[0]

            if ans == "N":
                raise RuntimeError("Interrotto dall'utente")

            query_job = self.client.query(query)
            data = query_job.result().to_dataframe()
            data.to_parquet(path=file_name, compression="gzip", index=False)

        return data

    def run_from_file(self, path):
        query = ""

        if not os.path.exists(path):
            raise RuntimeError("La path specificata non esiste")

        if os.stat(path).st_size == 0:
            raise RuntimeError("Il file specificato è vuoto")

        with open(path, 'r') as fp:
            query = fp.read()

        return self.run(query)

# to keep track of cache, use a file containing an object. each key is the hash of the query. the key points to an
# object containing teh configuration for the cache file (expire, format, ...)