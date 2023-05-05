from time import time

import backoff
import pandas as pd
import psycopg2 as pg

MAX_TRIES = 3


def timer_func(func):
    """
    Wrapper to get the execution time of the function object passed
    """

    def wrap_func(*args, **kwargs):
        t1 = time()
        result = func(*args, **kwargs)
        t2 = time()
        print(f"Function {func.__name__!r} executed in {(t2-t1):.4f}s")
        return result

    return wrap_func


class DBConnector:
    """
    Class to manage the connection to the DB
    """

    def __init__(self):
        """
        Init of the class
        """
        self.credentials = self.__get_db_credentials()

    def __get_db_credentials(self):
        """
        Read secret file to get db credentials
        """
        arr = []
        with open(".secrets/db_credentials") as FileObj:
            for lines in FileObj:
                line = lines.strip().replace("\n", "")
                arr.append(line)
        host_ip = arr[2]
        credentials = {
            "user": arr[0],
            "password": arr[1],
            "host": host_ip,
            "port": arr[3],
            "database": arr[4],
        }
        return credentials

    def create_df(self, cur, data_all, cast_float=True):
        """
        Convert query's result to pandas dataframe
        """
        cols = []
        for data_iter in cur.description:
            cols.append(data_iter[0])
        df_data = pd.DataFrame(data=data_all, columns=cols)
        if cast_float:
            df_data = df_data.apply(pd.to_numeric, errors="ignore")
        return df_data

    @backoff.on_exception(backoff.expo, Exception, max_tries=MAX_TRIES)
    def get_data_from_db(self, query: str, cast_float=True):
        """
        Get data from DB
        """
        DATABASE = self.credentials["database"]
        HOST = self.credentials["host"]
        PASSWORD = self.credentials["password"]
        PORT = self.credentials["port"]
        USER = self.credentials["user"]

        df_data = pd.DataFrame()
        try:
            conn = pg.connect(
                host=HOST, user=USER, password=PASSWORD, port=PORT, database=DATABASE
            )
            cur = conn.cursor()
            cur.itersize = 50000
            timezone = "America/Lima"
            cur.execute(f"SET TIME ZONE '{timezone}';")
            cur.execute(query)
            lst_data_fetched = cur.fetchmany(cur.itersize)
            while len(lst_data_fetched) > 0:
                df_aux = self.create_df(cur, lst_data_fetched, cast_float)
                df_data = pd.concat([df_data, df_aux], ignore_index=True)
                lst_data_fetched = cur.fetchmany(cur.itersize)
        except Exception as err:
            print("Error in get_data_from_db %s", err)
        finally:
            cur.close()
            conn.close()
            return df_data
