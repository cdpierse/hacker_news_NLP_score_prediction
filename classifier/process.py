import logging

import pandas as pd
import psycopg2 as pg
import tldextract
from string import punctuation
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
                logging.error(e + "Please ensure container is up and running.")

    def prepend_domain(self, r: pd.Series):
        url = r.url
        if url == "empty":
            r.title = "empty :- " + r.title
            return r.title
        else:
            domain = Process.extract_domain(url)
            r.title = domain.domain + " :- " + r.title
            return r.title

    def apply_title_transforms(self):
        logging.info("Applying title transformations")
        self.posts['title'] = self.posts['title'].apply(
            Process.remove_punctuation)
        self.posts['title'] = self.posts['title'].apply(Process.title_to_lower)
        self.posts['title'] = self.posts.apply(self.prepend_domain, axis=1)
        logging.info("Applied all title transforms")

    def apply_bucket_creation(self):
        self.posts["score_bands"] = self.posts.apply(
            self.create_class_buckets, axis=1)

    def create_class_buckets(self, r: pd.Series):
        if r.score <= 5:
            return '0-5'
        elif r.score <= 25:
            return '5-25'
        elif r.score <= 50:
            return '25-50'
        else:
            return '50+'

    def undersample(self,n: int, class_name: str):
        assert class_name in self.posts.score_bands.unique.values,
        f"class name: {class_name} not found in df"
        drop_rows = self.posts[self.posts.score_bands ==
                               class_name].sample(n=n)
        drop_idx = drop_rows.index
        self.posts.drop(drop_idx, axis=1)

    @staticmethod
    def title_to_lower(s: str):
        return s.lower()

    @staticmethod
    def remove_punctuation(s: str):
        return s.translate(str.maketrans('', '', punctuation))

    @staticmethod
    def extract_domain(url: str):
        try:
            return tldextract.extract(url)
        except Exception as e:
            logging.error(
                f"Could not extract url for {url}, returning empty string instead")
            return ""


if __name__ == "__main__":
    p = Process()
    p.apply_title_transforms()
