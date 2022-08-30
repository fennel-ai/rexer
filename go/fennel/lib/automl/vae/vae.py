################################################################################################################################
# This file is used as part of the pipeline definition and is not used directly in the code, but checked in for record keeping.
################################################################################################################################


import argparse
import boto3
import os

os.system("pip install tensorboard")
os.system("pip install s3-concat")

STATE_DICT_KEY = 'model_state_dict'
OPTIMIZER_STATE_DICT_KEY = 'optimizer_state_dict'
################################################################################################################################
# Dataset
################################################################################################################################

from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
from tqdm import tqdm
tqdm.pandas()

from abc import *
from pathlib import Path
import os
import tempfile
import shutil
import pickle
from s3_concat import S3Concat
import time

DATASET_FILENAME = 'vae-concat-dataset'

class VAEDataset(metaclass=ABCMeta):
    def __init__(self, args, s3client):
        self.args = args
        self.min_user_history = args.min_user_history
        self.min_item_count = args.min_item_count
        self.s3client = s3client
        self.current_time = time.strftime("%m-%d-%H-%M-%S.csv", time.localtime())
        now = datetime.utcnow()
        year = now.strftime("%Y")
        month = now.strftime("%m")
        day = now.strftime("%d")
        hour = now.strftime("%H")
        self.preprocess_path = f'preprocessed/year={year}/month={month}/day={day}/hour={hour}'
        self.dataset_filename = DATASET_FILENAME + self.current_time
        assert self.min_user_history >= 2, 'Need at least 2 ratings per user for validation and test'


    def concat_training_data(self):
        bucket = self.args.s3_bucket
        now = datetime.utcnow()
        year = now.strftime("%Y")
        month = now.strftime("%m")
        day = now.strftime("%d")
        hour = now.strftime("%H")
        concatenated_file = f'{self.args.data_dir}/{self.preprocess_path}/{self.dataset_filename}'
        min_file_size = None
        # Init the job
        job = S3Concat(bucket, concatenated_file, min_file_size,
                       content_type='test/csv',
                       session=boto3.session.Session(),  # For custom aws session
                       )
        print("Duration ", args.duration)
        for i in range(int(args.duration)):
            utc_past = now - timedelta(days=i)
            year_past = utc_past.strftime("%Y")
            month_past = utc_past.strftime("%m")
            day_past = utc_past.strftime("%d")
            path = f"{self.args.data_dir}/year={year_past}/month={month_past}/day={day_past}/"
            try:
                print("Adding path ", path)
                job.add_files(path)
            except Exception as e:
                print("Exception ", e)
        return job.concat(small_parts_threads=4)

    def load_ratings_df(self):
        data_path = self.concat_training_data()
        if data_path is None or len(data_path) == 0:
            raise Exception("No Data found")
        dataset_path = data_path[0]
        response = self.s3client.get_object(Bucket=self.args.s3_bucket, Key=str(dataset_path))
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            print(f"Successful S3 get_object response. Status - {status}")
            df = pd.read_csv(response.get("Body"))
            df.columns = ['uid', 'sid', 'timestamp']
            return df
        else:
            raise Exception(f"Unsuccessful S3 get_object response. Status - {status}")

    def load_dataset(self):
        self.preprocess()
        dataset_path = self._get_preprocessed_dataset_path()
        response = s3client.get_object(Bucket=self.args.s3_bucket, Key=str(dataset_path))
        body = response['Body'].read()
        dataset = pickle.loads(body)
        return dataset

    def preprocess(self):
        dataset_path = self._get_preprocessed_dataset_path()
        print("Trying for :", dataset_path)
        try:
            resp = self.s3client.head_object(Bucket=self.args.s3_bucket, Key=str(dataset_path))
            print('Already preprocessed. Skip preprocessing')
            return
        except Exception as e:
            df = self.load_ratings_df()
            #             df = self.make_implicit(df)
            df = self.filter_triplets(df)
            df, umap, smap = self.densify_index(df)
            train, val, test = self.split_df(df, len(umap))
            dataset = {'train': train,
                       'val': val,
                       'test': test,
                       'umap': umap,
                       'smap': smap}
            response = self.s3client.put_object(Bucket=self.args.s3_bucket, Key=str(dataset_path), Body=pickle.dumps(dataset))
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status == 200:
                print(f"Successful S3 put_object response. Status - {status}")
            else:
                raise Exception(f"Unsuccessful S3 put_object response. Status - {status}")

    #     def make_implicit(self, df):
    #         print('Turning into implicit ratings')
    #         df = df[df['rating'] >= self.min_rating]
    #         return df

    def filter_triplets(self, df):
        print('Filtering triplets')
        if self.min_item_count > 0:
            item_sizes = df.groupby('sid').size()
            good_items = item_sizes.index[item_sizes >= self.min_item_count]
            df = df[df['sid'].isin(good_items)]

        if self.min_user_history > 0:
            user_sizes = df.groupby('uid').size()
            good_users = user_sizes.index[user_sizes >= self.min_user_history]
            df = df[df['uid'].isin(good_users)]

        return df

    def densify_index(self, df):
        print('Densifying index')
        umap = {u: i for i, u in enumerate(set(df['uid']))}
        smap = {s: i for i, s in enumerate(set(df['sid']))}
        df['uid'] = df['uid'].map(umap)
        df['sid'] = df['sid'].map(smap)
        return df, umap, smap

    def split_df(self, df, user_count):
        print('Splitting')
        np.random.seed(self.args.dataset_split_seed)
        eval_set_size = self.args.eval_set_size
        print("User count:", user_count)

        # Generate user indices
        permuted_index = np.random.RandomState(seed=42).permutation(user_count)
        train_user_index = permuted_index[:-2*eval_set_size]
        val_user_index = permuted_index[-2*eval_set_size:-eval_set_size]
        test_user_index = permuted_index[-eval_set_size:]

        # Split DataFrames
        train_df = df.loc[df['uid'].isin(train_user_index)]
        val_df   = df.loc[df['uid'].isin(val_user_index)]
        test_df  = df.loc[df['uid'].isin(test_user_index)]

        # DataFrame to dict => {uid : list of sid's}
        train = dict(train_df.groupby('uid').progress_apply(lambda d: list(d.sort_values(by='timestamp')['sid'])))
        val = dict(val_df.groupby('uid').progress_apply(lambda d: list(d.sort_values(by='timestamp')['sid'])))
        test = dict(test_df.groupby('uid').progress_apply(lambda d: list(d.sort_values(by='timestamp')['sid'])))
        return train, val, test

    def _get_rawdata_root_path(self):
        return Path(self.args.data_dir)

    def _get_preprocessed_folder_path(self):
        preprocessed_root = self._get_rawdata_root_path()
        return preprocessed_root.joinpath(self.preprocess_path)

    def _get_preprocessed_dataset_path(self):
        folder = self._get_preprocessed_folder_path()
        return folder.joinpath('dataset.pkl')



################################################################################################################################
# Data Loader
################################################################################################################################

import random
import torch
import torch.utils.data as data_utils
from scipy import sparse
import numpy as np

class AbstractDataloader(metaclass=ABCMeta):
    def __init__(self, args, dataset):
        self.args = args
        seed = args.dataloader_random_seed
        self.rng = random.Random(seed)
        self.save_folder = dataset._get_preprocessed_folder_path()
        dataset = dataset.load_dataset()
        self.train = dataset['train']
        self.val = dataset['val']
        self.test = dataset['test']
        self.umap = dataset['umap']
        self.smap = dataset['smap']
        self.item2denseindex = self.smap
        self.og_items = set(k for k in dataset['smap'])
        self.user_count = len(self.umap)
        self.item_count = len(self.smap)


    @abstractmethod
    def get_pytorch_dataloaders(self):
        pass


class AEDataloader(AbstractDataloader):
    def __init__(self, args, dataset):
        super().__init__(args, dataset)

        # For autoencoders, we should remove users from val/test sets
        # that rated items NOT in the training set

        # extract a list of unique items from the training set
        unique_items = set()
        for items in self.train.values():
            unique_items.update(items)

        # Then, we remove users from the val/test set.
        self.val = {user : items for user, items in self.val.items() \
                    if all(item in unique_items for item in items)}
        self.test = {user : items for user, items in self.test.items() \
                     if all(item in unique_items for item in items)}

        # re-map items
        self.smap = {s : i for i, s in enumerate(unique_items)}
        self.denseindex2traindenseindex = self.smap
        remap = lambda items: [self.smap[item] for item in items]
        self.train = {user : remap(items) for user, items in self.train.items()}
        self.val = {user : remap(items) for user, items in self.val.items()}
        self.test = {user : remap(items) for user, items in self.test.items()}

        # some bookkeeping
        del self.umap, self.user_count
        self.item_count = len(unique_items)
        args.num_items = self.item_count


    def get_pytorch_dataloaders(self):
        train_loader = self._get_train_loader()
        val_loader = self._get_val_loader()
        test_loader = self._get_test_loader()
        return train_loader, val_loader, test_loader

    def _get_train_loader(self):
        dataset = self._get_train_dataset()
        dataloader = data_utils.DataLoader(dataset, batch_size=self.args.train_batch_size,
                                           shuffle=True, pin_memory=True)
        return dataloader

    def _get_train_dataset(self):
        dataset = AETrainDataset(self.train, item_count=self.item_count)
        return dataset

    def _get_val_loader(self):
        return self._get_eval_loader(mode='val')

    def _get_test_loader(self):
        return self._get_eval_loader(mode='test')

    def _get_eval_loader(self, mode):
        batch_size = self.args.val_batch_size if mode == 'val' else self.args.test_batch_size
        dataset = self._get_eval_dataset(mode)
        dataloader = data_utils.DataLoader(dataset, batch_size=batch_size,
                                           shuffle=False, pin_memory=True)
        return dataloader

    def _get_eval_dataset(self, mode):
        data = self.val if mode == 'val' else self.test
        dataset = AEEvalDataset(data, item_count=self.item_count)
        return dataset


class AETrainDataset(data_utils.Dataset):
    def __init__(self, user2items, item_count):
        # Row indices for sparse matrix
        #   e.g. [0, 0, 0, 1, 1, 4, 4, 4, 4]
        #        when user2items = {0:[1,2,3], 1:[4,5], 4:[6,7,8,9]}
        user_row = []
        for user, useritem in enumerate(user2items.values()):
            for _ in range(len(useritem)):
                user_row.append(user)

        # Column indices for sparse matrix
        item_col = []
        for useritem in user2items.values():
            item_col.extend(useritem)

        # Construct sparse matrix
        assert len(user_row) == len(item_col)
        sparse_data = sparse.csr_matrix((np.ones(len(user_row)), (user_row, item_col)),
                                        dtype='float64', shape=(len(user2items), item_count))

        # Convert to torch tensor
        self.data = torch.FloatTensor(sparse_data.toarray())

    def __len__(self):
        return self.data.shape[0]

    def __getitem__(self, index):
        return self.data[index]


class AEEvalDataset(data_utils.Dataset):
    def __init__(self, user2items, item_count):
        # Split each user's items to input and label s.t. the two are disjoint
        # Both are lists of np.ndarrays
        input_list, label_list = self.split_input_label_proportion(user2items)

        # Row indices for sparse matrix
        input_user_row, label_user_row = [], []
        for user, input_items in enumerate(input_list):
            for _ in range(len(input_items)):
                input_user_row.append(user)
        for user, label_items in enumerate(label_list):
            for _ in range(len(label_items)):
                label_user_row.append(user)
        input_user_row, label_user_row = np.array(input_user_row), np.array(label_user_row)

        # Column indices for sparse matrix
        input_item_col = np.hstack(input_list)
        label_item_col = np.hstack(label_list)

        # Construct sparse matrix
        sparse_input = sparse.csr_matrix((np.ones(len(input_user_row)), (input_user_row, input_item_col)),
                                         dtype='float64', shape=(len(input_list), item_count))
        sparse_label = sparse.csr_matrix((np.ones(len(label_user_row)), (label_user_row, label_item_col)),
                                         dtype='float64', shape=(len(label_list), item_count))

        # Convert to torch tensor
        self.input_data = torch.FloatTensor(sparse_input.toarray())
        self.label_data = torch.FloatTensor(sparse_label.toarray())

    def split_input_label_proportion(self, data, label_prop=0.2):
        input_list, label_list = [], []

        for items in data.values():
            items = np.array(items)
            if len(items) * label_prop >= 1:
                # ith item => "chosen for label" if choose_as_label[i] is True else "chosen for input"
                choose_as_label = np.zeros(len(items), dtype='bool')
                chosen_index = np.random.choice(len(items), size=int(label_prop * len(items)), replace=False).astype('int64')
                choose_as_label[chosen_index] = True
                input_list.append(items[np.logical_not(choose_as_label)])
                label_list.append(items[choose_as_label])
            else:
                input_list.append(items)
                label_list.append(np.array([]))

        return input_list, label_list

    def __len__(self):
        return len(self.input_data)

    def __getitem__(self, index):
        return self.input_data[index], self.label_data[index]


################################################################################################################################
# Model
################################################################################################################################

import torch
import torch.nn as nn
import torch.nn.functional as F

from abc import *


class BaseModel(nn.Module, metaclass=ABCMeta):
    def __init__(self, args):
        super().__init__()
        self.args = args

    @classmethod
    @abstractmethod
    def code(cls):
        pass




class VAEModel(BaseModel):
    def __init__(self, args):
        super().__init__(args)
        self.latent_dim = args.vae_latent_dim

        # Input dropout
        self.input_dropout = nn.Dropout(p=args.vae_dropout)

        # Construct a list of dimensions for the encoder and the decoder
        dims = [args.vae_hidden_dim] * 2 * args.vae_num_hidden
        dims = [args.num_items] + dims + [args.vae_latent_dim * 2]

        # Stack encoders and decoders
        encoder_modules, decoder_modules = [], []
        for i in range(len(dims)//2):
            encoder_modules.append(nn.Linear(dims[2*i], dims[2*i+1]))
            if i == 0:
                decoder_modules.append(nn.Linear(dims[-1]//2, dims[-2]))
            else:
                decoder_modules.append(nn.Linear(dims[-2*i-1], dims[-2*i-2]))
        self.encoder = nn.ModuleList(encoder_modules)
        self.decoder = nn.ModuleList(decoder_modules)

        # Initialize weights
        self.encoder.apply(self.weight_init)
        self.decoder.apply(self.weight_init)

    def weight_init(self, m):
        if isinstance(m, nn.Linear):
            nn.init.kaiming_normal_(m.weight)
            m.bias.data.zero_()

    @classmethod
    def code(cls):
        return 'vae'

    def forward(self, x):
        x = F.normalize(x)
        x = self.input_dropout(x)

        for i, layer in enumerate(self.encoder):
            x = layer(x)
            if i != len(self.encoder) - 1:
                x = torch.tanh(x)

        mu, logvar = x[:, :self.latent_dim], x[:, self.latent_dim:]

        if self.training:
            # since log(var) = log(sigma^2) = 2*log(sigma)
            sigma = torch.exp(0.5 * logvar)
            eps = torch.randn_like(sigma)
            x = mu + eps * sigma
        else:
            x = mu

        for i, layer in enumerate(self.decoder):
            x = layer(x)
            if i != len(self.decoder) - 1:
                x = torch.tanh(x)

        return x, mu, logvar


################################################################################################################################
# Train
################################################################################################################################


def train(args, export_root, s3client):
    os.environ['CUDA_VISIBLE_DEVICES'] = args.device_idx
    dataset = VAEDataset(args, s3client)
    datasetloader = AEDataloader(args, dataset)
    train_loader, val_loader, test_loader = datasetloader.get_pytorch_dataloaders()
    model = VAEModel(args)
    trainer = VAETrainer(args, model, train_loader, val_loader, test_loader, export_root)
    trainer.train()
    with open(os.path.join(args.model_dir, 'model.pth'), 'wb') as f:
        torch.save(model.state_dict(), f)
    with open(os.path.join(args.model_dir, 'items.pkl'), 'wb') as f:
        pickle.dump(datasetloader.og_items, f)
    with open(os.path.join(args.model_dir, 'item2denseindex.pkl'), 'wb') as f:
        pickle.dump(datasetloader.item2denseindex, f)
    with open(os.path.join(args.model_dir, 'denseindex2traindenseindex.pkl'), 'wb') as f:
        pickle.dump(datasetloader.denseindex2traindenseindex, f)

import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.tensorboard import SummaryWriter
from tqdm import tqdm
import torch.nn.functional as F

import json
from abc import *
from pathlib import Path


def recalls_and_ndcgs_for_ks(scores, labels, ks):
    metrics = {}

    scores = scores
    labels = labels
    answer_count = labels.sum(1)

    labels_float = labels.float()
    rank = (-scores).argsort(dim=1)
    cut = rank
    for k in sorted(ks, reverse=True):
        cut = cut[:, :k]
        hits = labels_float.gather(1, cut)
        metrics['Recall@%d' % k] = \
            (hits.sum(1) / torch.min(torch.Tensor([k]).to(labels.device), labels.sum(1).float())).mean().cpu().item()

        position = torch.arange(2, 2+k)
        weights = 1 / torch.log2(position.float())
        dcg = (hits * weights.to(hits.device)).sum(1)
        idcg = torch.Tensor([weights[:min(int(n), k)].sum() for n in answer_count]).to(dcg.device)
        ndcg = (dcg / idcg).mean()
        metrics['NDCG@%d' % k] = ndcg.cpu().item()
    return metrics

class VAETrainer(metaclass=ABCMeta):
    def __init__(self, args, model, train_loader, val_loader, test_loader, export_root):
        self.args = args
        self.device = args.device
        self.model = model.to(self.device)
        self.is_parallel = args.num_gpu > 1
        if self.is_parallel:
            self.model = nn.DataParallel(self.model)

        self.train_loader = train_loader
        self.val_loader = val_loader
        self.test_loader = test_loader
        self.optimizer = self._create_optimizer()
        if args.enable_lr_schedule:
            self.lr_scheduler = optim.lr_scheduler.StepLR(self.optimizer, step_size=args.decay_step, gamma=args.gamma)

        self.num_epochs = args.num_epochs
        self.metric_ks = args.metric_ks
        self.best_metric = args.best_metric

        self.export_root = export_root
        self.writer, self.train_loggers, self.val_loggers = self._create_loggers()
        self.add_extra_loggers()
        self.logger_service = LoggerService(self.train_loggers, self.val_loggers)
        self.log_period_as_iter = args.log_period_as_iter

        # Finding or using given optimal beta
        self.__beta = 0.0
        self.finding_best_beta = args.find_best_beta
        self.anneal_amount = 1.0 / args.total_anneal_steps
        if self.finding_best_beta:
            self.current_best_metric = 0.0
            self.anneal_cap = 1.0
        else:
            self.anneal_cap = args.anneal_cap

    def add_extra_loggers(self):
        cur_beta_logger = MetricGraphPrinter(self.writer, key='cur_beta', graph_name='Beta', group_name='Train')
        self.train_loggers.append(cur_beta_logger)

        if self.args.find_best_beta:
            best_beta_logger = MetricGraphPrinter(self.writer, key='best_beta', graph_name='Best_beta', group_name='Validation')
            self.val_loggers.append(best_beta_logger)

    def log_extra_train_info(self, log_data):
        log_data.update({'cur_beta': self.__beta})

    def log_extra_val_info(self, log_data):
        if self.finding_best_beta:
            print("This is wierd ", self.finding_best_beta)
            log_data.update({'best_beta': self.best_beta})

    def train(self):
        accum_iter = 0
        self.validate(0, accum_iter)
        for epoch in range(self.num_epochs):
            accum_iter = self.train_one_epoch(epoch, accum_iter)
            self.validate(epoch, accum_iter)
        self.logger_service.complete({
            'state_dict': (self._create_state_dict()),
        })
        self.writer.close()

    def train_one_epoch(self, epoch, accum_iter):
        self.model.train()
        if self.args.enable_lr_schedule:
            self.lr_scheduler.step()

        average_meter_set = AverageMeterSet()
        tqdm_dataloader = tqdm(self.train_loader)

        for batch_idx, batch in enumerate(tqdm_dataloader):
            batch_size = batch[0].size(0)
            batch = [x.to(self.device) for x in batch]

            self.optimizer.zero_grad()
            loss = self.calculate_loss(batch)
            loss.backward()

            self.optimizer.step()

            average_meter_set.update('loss', loss.item())
            tqdm_dataloader.set_description(
                'Epoch {}, loss {:.3f} '.format(epoch+1, average_meter_set['loss'].avg))

            accum_iter += batch_size

            if self._needs_to_log(accum_iter):
                tqdm_dataloader.set_description('Logging to Tensorboard')
                log_data = {
                    'state_dict': (self._create_state_dict()),
                    'epoch': epoch+1,
                    'accum_iter': accum_iter,
                }
                log_data.update(average_meter_set.averages())
                self.log_extra_train_info(log_data)
                self.logger_service.log_train(log_data)

        return accum_iter

    def validate(self, epoch, accum_iter):
        self.model.eval()

        average_meter_set = AverageMeterSet()

        with torch.no_grad():
            tqdm_dataloader = tqdm(self.val_loader)
            for batch_idx, batch in enumerate(tqdm_dataloader):
                batch = [x.to(self.device) for x in batch]

                metrics = self.calculate_metrics(batch)

                for k, v in metrics.items():
                    average_meter_set.update(k, v)
                description_metrics = ['NDCG@%d' % k for k in self.metric_ks[:3]] + \
                                      ['Recall@%d' % k for k in self.metric_ks[:3]]
                description = 'Val: ' + ', '.join(s + ' {:.3f}' for s in description_metrics)
                description = description.replace('NDCG', 'N').replace('Recall', 'R')
                description = description.format(*(average_meter_set[k].avg for k in description_metrics))
                tqdm_dataloader.set_description(description)

            log_data = {
                'state_dict': (self._create_state_dict()),
                'epoch': epoch+1,
                'accum_iter': accum_iter,
            }
            log_data.update(average_meter_set.averages())
            self.log_extra_val_info(log_data)
            self.logger_service.log_val(log_data)

    def test(self):
        print('Test best model with test set!')

        best_model = torch.load(os.path.join(self.export_root, 'models', 'best_acc_model.pth')).get('model_state_dict')
        self.model.load_state_dict(best_model)
        self.model.eval()

        average_meter_set = AverageMeterSet()

        with torch.no_grad():
            tqdm_dataloader = tqdm(self.test_loader)
            for batch_idx, batch in enumerate(tqdm_dataloader):
                batch = [x.to(self.device) for x in batch]

                metrics = self.calculate_metrics(batch)

                for k, v in metrics.items():
                    average_meter_set.update(k, v)
                description_metrics = ['NDCG@%d' % k for k in self.metric_ks[:3]] + \
                                      ['Recall@%d' % k for k in self.metric_ks[:3]]
                description = 'Val: ' + ', '.join(s + ' {:.3f}' for s in description_metrics)
                description = description.replace('NDCG', 'N').replace('Recall', 'R')
                description = description.format(*(average_meter_set[k].avg for k in description_metrics))
                tqdm_dataloader.set_description(description)

            average_metrics = average_meter_set.averages()
            with open(os.path.join(self.export_root, 'logs', 'test_metrics.json'), 'w') as f:
                json.dump(average_metrics, f, indent=4)
            print(average_metrics)

    def _create_optimizer(self):
        args = self.args
        if args.optimizer.lower() == 'adam':
            return optim.Adam(self.model.parameters(), lr=args.lr, weight_decay=args.weight_decay)
        elif args.optimizer.lower() == 'sgd':
            return optim.SGD(self.model.parameters(), lr=args.lr, weight_decay=args.weight_decay, momentum=args.momentum)
        else:
            raise ValueError

    def _create_loggers(self):
        root = Path(self.export_root)
        writer = SummaryWriter(root.joinpath('logs'))
        model_checkpoint = root.joinpath('models')

        train_loggers = [
            MetricGraphPrinter(writer, key='epoch', graph_name='Epoch', group_name='Train'),
            MetricGraphPrinter(writer, key='loss', graph_name='Loss', group_name='Train'),
        ]

        val_loggers = []
        for k in self.metric_ks:
            val_loggers.append(
                MetricGraphPrinter(writer, key='NDCG@%d' % k, graph_name='NDCG@%d' % k, group_name='Validation'))
            val_loggers.append(
                MetricGraphPrinter(writer, key='Recall@%d' % k, graph_name='Recall@%d' % k, group_name='Validation'))
        val_loggers.append(RecentModelLogger(model_checkpoint))
        val_loggers.append(BestModelLogger(model_checkpoint, metric_key=self.best_metric))
        return writer, train_loggers, val_loggers

    def _create_state_dict(self):
        return {
            STATE_DICT_KEY: self.model.module.state_dict() if self.is_parallel else self.model.state_dict(),
            OPTIMIZER_STATE_DICT_KEY: self.optimizer.state_dict(),
        }

    def _needs_to_log(self, accum_iter):
        return accum_iter % self.log_period_as_iter < self.args.train_batch_size and accum_iter != 0


    @property
    def beta(self):
        if self.model.training:
            self.__beta = min(self.__beta + self.anneal_amount, self.anneal_cap)
        return self.__beta

    def calculate_loss(self, batch):
        input_x = torch.stack(batch)
        recon_x, mu, logvar = self.model(input_x)
        CE = -torch.mean(torch.sum(F.log_softmax(recon_x, 1) * input_x, -1))
        KLD = -0.5 * torch.mean(torch.sum(1 + logvar - mu.pow(2) - logvar.exp(), dim=1))

        return CE + self.beta * KLD

    def calculate_metrics(self, batch):
        inputs, labels = batch
        logits, _, _ = self.model(inputs)
        logits[inputs!=0] = -float("Inf") # IMPORTANT: remove items that were in the input
        metrics = recalls_and_ndcgs_for_ks(logits, labels, self.metric_ks)

        # Annealing beta
        if self.finding_best_beta:
            if self.current_best_metric < metrics[self.best_metric]:
                self.current_best_metric = metrics[self.best_metric]
                self.best_beta = self.__beta

        return metrics

################################################################################################################################
# Loggers
################################################################################################################################


import os
from abc import ABCMeta, abstractmethod

import torch


def save_state_dict(state_dict, path, filename):
    torch.save(state_dict, os.path.join(path, filename))


class LoggerService(object):
    def __init__(self, train_loggers=None, val_loggers=None):
        self.train_loggers = train_loggers if train_loggers else []
        self.val_loggers = val_loggers if val_loggers else []

    def complete(self, log_data):
        for logger in self.train_loggers:
            logger.complete(**log_data)
        for logger in self.val_loggers:
            logger.complete(**log_data)

    def log_train(self, log_data):
        for logger in self.train_loggers:
            logger.log(**log_data)

    def log_val(self, log_data):
        for logger in self.val_loggers:
            logger.log(**log_data)


class AbstractBaseLogger(metaclass=ABCMeta):
    @abstractmethod
    def log(self, *args, **kwargs):
        raise NotImplementedError

    def complete(self, *args, **kwargs):
        pass


class RecentModelLogger(AbstractBaseLogger):
    def __init__(self, checkpoint_path, filename='checkpoint-recent.pth'):
        self.checkpoint_path = checkpoint_path
        if not os.path.exists(self.checkpoint_path):
            os.mkdir(self.checkpoint_path)
        self.recent_epoch = None
        self.filename = filename

    def log(self, *args, **kwargs):
        epoch = kwargs['epoch']

        if self.recent_epoch != epoch:
            self.recent_epoch = epoch
            state_dict = kwargs['state_dict']
            state_dict['epoch'] = kwargs['epoch']
            save_state_dict(state_dict, self.checkpoint_path, self.filename)

    def complete(self, *args, **kwargs):
        save_state_dict(kwargs['state_dict'], self.checkpoint_path, self.filename + '.final')


class BestModelLogger(AbstractBaseLogger):
    def __init__(self, checkpoint_path, metric_key='mean_iou', filename='best_acc_model.pth'):
        self.checkpoint_path = checkpoint_path
        if not os.path.exists(self.checkpoint_path):
            os.mkdir(self.checkpoint_path)

        self.best_metric = 0.
        self.metric_key = metric_key
        self.filename = filename

    def log(self, *args, **kwargs):
        current_metric = kwargs[self.metric_key]
        if self.best_metric < current_metric:
            print("Update Best {} Model at {}".format(self.metric_key, kwargs['epoch']))
            self.best_metric = current_metric
            save_state_dict(kwargs['state_dict'], self.checkpoint_path, self.filename)


class MetricGraphPrinter(AbstractBaseLogger):
    def __init__(self, writer, key='train_loss', graph_name='Train Loss', group_name='metric'):
        self.key = key
        self.graph_label = graph_name
        self.group_name = group_name
        self.writer = writer

    def log(self, *args, **kwargs):
        if self.key in kwargs:
            self.writer.add_scalar(self.group_name + '/' + self.graph_label, kwargs[self.key], kwargs['accum_iter'])
        else:
            self.writer.add_scalar(self.group_name + '/' + self.graph_label, 0, kwargs['accum_iter'])

    def complete(self, *args, **kwargs):
        self.writer.close()

################################################################################################################################
# Utils
################################################################################################################################


class AverageMeterSet(object):
    def __init__(self, meters=None):
        self.meters = meters if meters else {}

    def __getitem__(self, key):
        if key not in self.meters:
            meter = AverageMeter()
            meter.update(0)
            return meter
        return self.meters[key]

    def update(self, name, value, n=1):
        if name not in self.meters:
            self.meters[name] = AverageMeter()
        self.meters[name].update(value, n)

    def reset(self):
        for meter in self.meters.values():
            meter.reset()

    def values(self, format_string='{}'):
        return {format_string.format(name): meter.val for name, meter in self.meters.items()}

    def averages(self, format_string='{}'):
        return {format_string.format(name): meter.avg for name, meter in self.meters.items()}

    def sums(self, format_string='{}'):
        return {format_string.format(name): meter.sum for name, meter in self.meters.items()}

    def counts(self, format_string='{}'):
        return {format_string.format(name): meter.count for name, meter in self.meters.items()}

class AverageMeter(object):
    """Computes and stores the average and current value"""

    def __init__(self):
        self.val = 0
        self.avg = 0
        self.sum = 0
        self.count = 0

    def reset(self):
        self.val = 0
        self.avg = 0
        self.sum = 0
        self.count = 0

    def update(self, val, n=1):
        self.val = val
        self.sum += val
        self.count += n
        self.avg = self.sum / self.count

    def __format__(self, format):
        return "{self.val:{format}} ({self.avg:{format}})".format(self=self, format=format)


################################################################################################################################
# Main
###############################################################################################################################

from collections import defaultdict
import pickle
import json
from scipy import sparse

class ModelArgs:
    model_code='vae'
    model_init_seed=0
    num_items=0
    vae_num_hidden=1
    vae_hidden_dim=600
    vae_latent_dim=200
    vae_dropout=0.5

def model_fn(model_dir):
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    with open(os.path.join(model_dir, 'items.pkl'), 'rb') as f:
        og_items = pickle.load(f)
    args = ModelArgs()
    args.num_items = len(og_items)
    model = VAEModel(args)
    with open(os.path.join(model_dir, 'model.pth'), 'rb') as f:
        if device == torch.device('cpu'):
            model.load_state_dict(torch.load(f, map_location=torch.device('cpu')))
        else:
            model.load_state_dict(torch.load(f))

    model.to(device).eval()

    with open(os.path.join(model_dir, 'item2denseindex.pkl'), 'rb') as f:
        item2denseindex = pickle.load(f)

    with open(os.path.join(model_dir, 'denseindex2traindenseindex.pkl'), 'rb') as f:
        denseindex2traindenseindex = pickle.load(f)

    denseindex2item = {}
    for k, v in item2denseindex.items():
        denseindex2item[v] = k

    traindenseindex2denseindex = {}
    for k, v in denseindex2traindenseindex.items():
        traindenseindex2denseindex[v] = k

    return model, og_items, item2denseindex, denseindex2traindenseindex, denseindex2item, traindenseindex2denseindex

def transform_fn(model_input, request_body, request_content_type,
                 response_content_type='application/json'):

    """Run prediction and return the output.
    The function
    1. Pre-processes the input request
    2. Runs prediction
    3. Post-processes the prediction output.
    """
    # preprocess
    if request_content_type == 'application/json':
        data = json.loads(request_body)
        model, og_items, item2denseindex, denseindex2traindenseindex, denseindex2item, traindenseindex2denseindex = model_input
        user2item_history = data.pop("user_histories", data)
        user2items = []
        for user, useritem in enumerate(user2item_history):
            user2items.append([x for x in useritem if x in og_items])

        user_row = []
        for user, useritem in enumerate(user2items):
            for _ in range(len(useritem)):
                user_row.append(user)

        # Column indices for sparse matrix
        item_col = []
        for useritems in user2items:
            item_col.extend(denseindex2traindenseindex[item2denseindex[item]] for item in useritems)
        sparse_data = sparse.csr_matrix((np.ones(len(user_row)), (user_row, item_col)),
                                        dtype='float64', shape=(len(user2items), len(og_items)))

        # Convert to torch tensor
        input_tensor = torch.FloatTensor(sparse_data.toarray())
        result = {}
        with torch.no_grad():
            model.eval()
            logits, mu, logvar = model(input_tensor)
            ps =  torch.nn.functional.softmax(logits, dim=1)

        if data.pop('get_embedding', data):
            result['embedding'] = mu.detach().cpu().numpy().tolist()
        else:
            limit = int(data.pop('limit', data))
            ps_np = ps.detach().cpu().numpy()#.argsort(axis=1)[:][-limit:][::-1]
            top_k_inds = np.argsort(ps_np, axis=1)[:, -limit:]
            top_k = np.take_along_axis(ps_np, top_k_inds, axis=-1)[:, -limit:]
            top_k_pairs = np.stack((top_k_inds, top_k), axis=2)
            recommendations = []
            for res in top_k_pairs.tolist():
                recommendations.append([(denseindex2item[traindenseindex2denseindex[int(r[0])]], r[1]) for r in res][::-1])
            result['recommendation'] = recommendations
        return json.dumps(result), response_content_type

    raise Exception(f'Requested unsupported ContentType in content_type {request_content_type}')
def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    ################
    # Dataset
    ################
    parser.add_argument('--duration', type=int, default=14, help='Number of days of training data to be concatenated')

    parser.add_argument('--min_user_history', type=int, default=5, help='Only keep users with more than min_user_history ratings')
    parser.add_argument('--min_item_count', type=int, default=0, help='Only keep items with more than min_item_count ratings')
    parser.add_argument('--dataset_split_seed', type=int, default=98765)
    parser.add_argument('--eval_set_size', type=int, default=500,
                        help='Size of val and test set. 500 for ML-1m and 10000 for ML-20m recommended')
    parser.add_argument("--data_dir", type=str, default="movielens")
    parser.add_argument("--s3_bucket", type=str, default="vae-training-data")
    parser.add_argument("--data_interaction_file", type=str, default="ratings.csv")

    ################
    # Dataloader
    ################
    parser.add_argument('--dataloader_random_seed', type=float, default=0.0)
    parser.add_argument('--train_batch_size', type=int, default=128)
    parser.add_argument('--val_batch_size', type=int, default=128)
    parser.add_argument('--test_batch_size', type=int, default=128)


    ################
    # Trainer
    ################
    # device #
    parser.add_argument('--device', type=str, default='cpu', choices=['cpu', 'cuda'])
    parser.add_argument("--num_gpu", type=int, default=os.environ["SM_NUM_GPUS"])
    parser.add_argument('--device_idx', type=str, default='0')
    # optimizer #
    parser.add_argument('--optimizer', type=str, default='Adam', choices=['SGD', 'Adam'])
    parser.add_argument('--enable_lr_schedule', type=str2bool, nargs='?', default=False)
    parser.add_argument('--lr', type=float, default=0.001, help='Learning rate')
    parser.add_argument('--weight_decay', type=float, default=0.01, help='l2 regularization')
    parser.add_argument('--momentum', type=float, default=0.0, help='SGD momentum')
    # lr scheduler #
    parser.add_argument('--decay_step', type=int, default=15, help='Decay step for StepLR')
    parser.add_argument('--gamma', type=float, default=0.1, help='Gamma for StepLR')
    # epochs #
    parser.add_argument('--num_epochs', type=int, default=50, help='Number of epochs for training')
    # logger #
    parser.add_argument('--log_period_as_iter', type=int, default=12800)
    # evaluation #
    parser.add_argument('--metric_ks', nargs='+', type=int, default=[10, 50, 100], help='ks for Metric@k')
    parser.add_argument('--best_metric', type=str, default='NDCG@10', help='Metric for determining the best model')
    # Finding optimal beta for VAE #
    parser.add_argument('--find_best_beta', type=str2bool, nargs='?', default=False,
                        help='If set True, the trainer will anneal beta all the way up to 1.0 and find the best beta')
    parser.add_argument('--total_anneal_steps', type=int, default=2000, help='The step number when beta reaches 1.0')
    parser.add_argument('--anneal_cap', type=float, default=0.25, help='Upper limit of increasing beta. Set this as the best beta found')

    ################
    # Model
    ################
    parser.add_argument('--model_init_seed', type=int, default=None)

    # VAE #
    parser.add_argument('--vae_num_items', type=int, default=None, help='Number of total items')
    parser.add_argument('--vae_num_hidden', type=int, default=1, help='Number of hidden layers in VAE')
    parser.add_argument('--vae_hidden_dim', type=int, default=600, help='Dimension of hidden layer in VAE')
    parser.add_argument('--vae_latent_dim', type=int, default=200, help="Dimension of latent vector in VAE (K in paper)")
    parser.add_argument('--vae_dropout', type=float, default=0.5, help='Probability of input dropout in VAE')

    ################
    # Experiment
    ################
    parser.add_argument('--experiment_dir', type=str, default='experiments')
    parser.add_argument('--experiment_description', type=str, default='test')


    # Container environment
    parser.add_argument("--model_dir", type=str, default=os.environ["SM_MODEL_DIR"])
    parser.add_argument("--output_data_dir", type=str, default=os.environ["SM_OUTPUT_DATA_DIR"])

    args, unknown = parser.parse_known_args()
    cred = boto3.Session().get_credentials()
    ACCESS_KEY = cred.access_key
    SECRET_KEY = cred.secret_key
    SESSION_TOKEN = cred.token  ## optional

    s3client = boto3.client('s3',
                            aws_access_key_id = ACCESS_KEY,
                            aws_secret_access_key = SECRET_KEY,
                            aws_session_token = SESSION_TOKEN
                            )

    print(args)
    train(args, args.output_data_dir, s3client)
