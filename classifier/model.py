import logging
import os

import pytorch_lightning as pl
from torch.utils.data import Dataset
from tqdm import tqdm
from transformers import (CONFIG_NAME, WEIGHTS_NAME, AdamW,
                          AutoModelForSequenceClassification, AutoTokenizer,
                          DistilBertConfig,
                          DistilBertForSequenceClassification,
                          DistilBertTokenizer, DistilBertTokenizerFast,
                          PreTrainedModel, PreTrainedTokenizer,
                          get_linear_schedule_with_warmup)
import pickle
from process import Process

CACHE_PATH = "classifier/cache/"


class HackerNewsPostDataset(Dataset):

    "Represents the dataset class for handling hacker news post data at train time"

    def __init__(self,
                 tokenizer: PreTrainedTokenizer,
                 split: str = "train",
                 block_size: int = 512,
                 overwrite_cache: bool = False):
        """[summary]

        Args:
            tokenizer (PreTrainedTokenizer): [description]
            split (str, optional): [description]. Defaults to "train".
            block_size (int, optional): [description]. Defaults to 512.
            overwrite_cache (bool, optional): [description]. Defaults to False.
        """
        assert os.path.isfile(CACHE_PATH + split + ".pkl")
        object_file_path = CACHE_PATH + split + ".pkl"
        directory, _ = os.path.split(object_file_path)
        post_object = Process.load_sample(split + ".pkl")

        block_size = block_size - (
            tokenizer.max_len - tokenizer.max_len_single_sentence
        )

        cached_features_file = os.path.join(
            directory, "distilbert_" + str(block_size) + "_" + split
        )
        self.posts_tokenized = []
        if os.path.exists(cached_features_file) and not overwrite_cache:
            logging.info(
                f"Loading features from your cached file {cached_features_file}"
            )
            with open(cached_features_file, "rb") as cache:
                self.posts_tokenized = pickle.load(cache)

        else:
            logging.info(
                f"Creating tokenized posts from file '{object_file_path}'")
            posts_text = list(post_object.get(split+'_text'))
            tokenized_text = []
            self.posts_tokenized = [tokenizer.batch_encode_plus(
                [text], is_pretokenized=False,
                max_length=block_size,
                pad_to_max_length=True,
                return_overflowing_tokens=False)
                for text in tqdm(posts_text)]

            logging.info(
                f"Saving tokenzied posts into cache file at '{cached_features_file}''")

        with open(cached_features_file, "wb") as cache:
            pickle.dump(self.posts_tokenized,
                        cache,
                        protocol=pickle.HIGHEST_PROTOCOL)

        self.labels = post_object.get(
            split+'_labels')

    def __len__(self):
        return len(self.posts_tokenized)

    def __getitem__(self, item):
        return (
            torch.tensor(
                self.posts_tokenized[item]['input_ids'], dtype=torch.long),
            torch.tensor(
                self.posts_tokenized[item]['attention_mask'], dtype=torch.long),
            self.labels[item]
        )


class HNPostClassifier(pl.LightningModule):

    def __init__(self, model: PreTrainedModel, tokenizer: PreTrainedTokenizer):
        super().__init__()
        self.model, self.tokenizer = (
            model,
            tokenizer
        )

    def forward(self, inputs):
        return self.model(**inputs)

    def training_step(self, batch, batch_num):
        input_ids, attention_mask, labels = batch
        outputs = self(
            {"input_ids": input_ids, "attention_mask": attention_mask, "labels": batch})
        loss, _ = outputs
        return {"loss": loss, "log": {"Loss": loss}}

        pass

    def optimizer_step(self):
        pass

    pass


if __name__ == "__main__":
    tokenizer = DistilBertTokenizer.from_pretrained('distilbert-base-uncased')
    posts = HackerNewsPostDataset(tokenizer=tokenizer)
