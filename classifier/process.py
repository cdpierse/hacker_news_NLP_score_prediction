import logging
from string import punctuation

import pandas as pd
import psycopg2 as pg
import tldextract
from sklearn.model_selection import train_test_split
import pickle
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

    def create_label_arrays(self):
        """
        create_label_arrays creates a one hot encoded
        version of our target score buckets. This method
        results in `self.label_arrays` which is an array of vectors
        each of with a length of 4. It also creates `self.label_columns_ordering`
        which maintains the original semantic meaning of each bucket category.
        """
        score_bucket_labels = self.posts['score_bands'].to_list()
        ohe = pd.get_dummies(score_bucket_labels)
        self.label_columns_ordered = ohe.columns.to_list()
        self.label_arrays = ohe.values
        logging.info("One hot encoding of score bucket labels completed")

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

    def split(self):
        """
        split creates train, test, and validation set splits
        for the hacker news post data. On the first pass it creates
        train and test splits. x_train is then split again to
        create a validation split. Stratification is applied on the
        labels to create representative samples.
        """
        x = self.posts['title']
        y = self.label_arrays
        self.x_train, self.x_test, self.y_train, self.y_test = train_test_split(
            x, y, test_size=0.1, random_state=0, stratify=y)
        self.x_train, self.x_val, self.y_train, self.y_val = train_test_split(
            self.x_train, self.y_train, test_size=0.10, random_state=0, stratify=self.y_train)
        logging.info(
            f"Succesfully split data. Train size = {len(self.x_train)} "
            f"Test Size = {len(self.x_test)}, Val Size = {len(self.x_val)}")

    def save_splits(self, path: str = "classifier/cache/"):
        """
            save_splits_to_disk saves train, test, and validation splits
            to file along with a copy of the original label column names.
            Args:
                path (str, optional): Path at which to save the splits. Defaults to 'classifier/cache/'.
        """

        def save(name: str, obj: dict):
            with open(path + name + ".pkl", 'wb') as f:
                pickle.dump(obj, f)
            logging.info(f"Saved {name}.pkl to file at {path}")

        try:
            train = {"train_text": self.x_train, "train_labels": self.y_train,
                     "label_names": self.label_columns_ordered}
            test = {"test_text": self.x_test, "test_labels": self.y_test,
                    "label_names": self.label_columns_ordered}
            validation = {"val_text": self.x_val, "val_labels": self.y_val,
                          "label_names": self.label_columns_ordered}
            save('train', train)
            save('test', test)
            save('val', validation)

        except Exception as e:
            logging.info(
                """"Unable to save your splits to disk. Please ensure
                that you have already created the splits using the `self.split()` method
                    """)
            raise

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
    p.apply_bucket_creation()
    p.set_undersample_n()
    p.undersample(n=p.UNDERSAMPLE_N, class_name=p.UNDERSAMPLE_CLASS)
    p.apply_title_transforms()
    p.create_label_arrays()
    p.split()
    p.save_splits()
    print(p.posts.score_bands.value_counts())
