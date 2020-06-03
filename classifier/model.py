from torch.utils.data import dataset
import pytorch_lightning as pl

class HackerNewsPostDataset(dataset):
    def __init__(self):
        """__init__ will be the dataset class for handling hacker
        news post data.
        """