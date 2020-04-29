import pandas as pd
import logging
import psycopg2 as pg

logging.basicConfig(level=logging.DEBUG)

CONFIG = {
    "host": "localhost",
    "port": 54320,
    "username": "postgres",
    "password": "password",
    "dbname": "hn_db",

}


class Connection:
    """ 
    Creates DB connection using context manager
    
    """

    def __init__(self, config=CONFIG):
        self.config = config

    def __enter__(self):
        logging.info(f"Creating connection to {self.config['dbname']}...")
        try:
            self.conn = pg.connect(
                dbname=self.config['dbname'],
                user=self.config['username'],
                password=self.config['password'],
                host=self.config['host'],
                port=self.config['port']
            )
            logging.info("Successfully connected.")
            return self.conn
        except Exception as e:
            logging.info(
                "Could not connect to database. Please ensure database container is up and running. ", e)

    def __exit__(self, type, value, traceback):
        logging.info("Closing DB Connection.")
        self.conn.close()


 