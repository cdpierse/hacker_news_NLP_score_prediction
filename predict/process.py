import logging

import pandas as pd
import psycopg2 as pg
import tldextract
from db import Connection


class Process:

    def __init__(self):
        self.get_posts_from_db()

    def get_posts_from_db(self):
        with Connection() as conn:
            try:
                dtypes = {}
                self.posts = pd.read_sql(
                    "SELECT * FROM posts", conn, parse_dates=['timestamp'])
                self.posts = self.posts.convert_dtypes()
                logging.info("Retrieved posts from database")
            except Exception as e:
                logging.error(e)

    @staticmethod
    def domain_extractor(url):
        try:
            return tldextract.extract(url)
        except Exception as e:
            logging.error(
                f"Could not extract url for {url}, returning empty string instead")
            return ""


if __name__ == "__main__":
    # p = Process()
    print(Process.domain_extractor("https://www.linkedin.com/"))
