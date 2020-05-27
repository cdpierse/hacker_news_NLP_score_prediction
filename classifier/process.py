import logging

import pandas as pd
import psycopg2 as pg
import tldextract
from string import punctuation
from db import Connection


class Process:

    def __init__(self, sample: bool = False):
        self.get_posts_from_db()
        if sample:
            self.posts = self.posts.sample(n=100_000)

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
        """Prepends the domain name of a posts
        link (if it exists) to the post title. If no domain exists 
        for a particular post row then a filler empty field is prepended. 

        Args:
            r (pd.Series): Row from df to apply transforms. 

        Returns:
            [str]: transformed row title with domain prepended. 
        """
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
        logging.info("Applying class bucket transformations")
        self.posts["score_bands"] = self.posts.apply(
            self.create_class_buckets, axis=1)
        logging.info("Class buckets created")

    def create_class_buckets(self, r: pd.Series):
        """Creates 4 discrete classes for ranges
        of post scores. Ranges are 
            -  "0-5"
            -  "5-25"
            -  "25-50"
            -  "50+"
        Args:
            r (pd.Series): Row on which to apply transforms

        Returns:
            [str]: String representing the class to which a score
            belongs.
        """
        if r.score <= 5:
            return '0-5'
        elif r.score <= 25:
            return '5-25'
        elif r.score <= 50:
            return '25-50'
        else:
            return '50+'

    def undersample(self,  n: int,  class_name: str, frac: float = None):
        """Undersamples the posts df according the score_band with
        `class_name` (usually "0-5")
        by either `n` or `frac` which are mutually exclusive. If `n` is given
        the dataframe is sampled by dropping n rows of `class_name`. Otherwise
        if `frac` is given that fraction of class_name is dropped.

        Args:
            n (int): Number of rows to drop
            frac (float): Fraction of rows to drop
            class_name (str): score_band class of rows to drop
        """
        if n and frac:
            n = None
        assert class_name in self.posts.score_bands.unique(),\
            f"class name: {class_name} not found in df"
        drop_rows = self.posts[self.posts.score_bands ==
                               class_name].sample(n=n, frac=frac)
        drop_idx = drop_rows.index
        self.posts = self.posts.drop(drop_idx, axis=0)

    def set_undersample_n(self):
        length = len(self.posts)
        num_unique_bands = self.posts.score_bands.nunique()
        top_class = self.posts.score_bands.value_counts()
        print(top_class)
        top_class_size, top_class_name = top_class.values[0], top_class.index[0]
        difference = length - top_class_size
        new_class_size = difference/num_unique_bands
        self.UNDERSAMPLE_N = int(top_class_size-new_class_size)
        self.UNDERSAMPLE_CLASS = top_class_name

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
    p = Process(sample=True)
    p.apply_bucket_creation()
    p.set_undersample_n()
    p.undersample(n=p.UNDERSAMPLE_N, class_name=p.UNDERSAMPLE_CLASS)
    p.apply_title_transforms()
    print(p.posts.score_bands.value_counts())
