#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jul 23 19:22:22 2023

@author: green-machine
"""

import io
from dataclasses import dataclass
from enum import Enum
from http import HTTPStatus
from typing import Any, Union

import requests
from core.config import DATA_DIR


class Dataset(str, Enum):

    def __new__(cls, value: str, usecols: range):
        obj = str.__new__(cls)
        obj._value_ = value
        obj.usecols = usecols
        return obj

    DOUGLAS = 'dataset_douglas.zip', range(4, 7)
    USA_BROWN = 'dataset_usa_brown.zip', range(5, 8)
    USA_COBB_DOUGLAS = 'dataset_usa_cobb-douglas.zip', range(5, 8)
    USA_KENDRICK = 'dataset_usa_kendrick.zip', range(4, 7)
    USA_MC_CONNELL = 'dataset_usa_mc_connell_brue.zip', range(1, 4)
    USCB = 'dataset_uscb.zip', range(9, 12)

    def get_kwargs(self) -> dict[str, Any]:

        NAMES = ['series_id', 'period', 'value']

        return {
            'filepath_or_buffer': DATA_DIR.joinpath(self.value),
            'header': 0,
            'names': NAMES,
            'index_col': 1,
            'skiprows': (0, 4)[self.name in ['USA_BROWN']],
            'usecols': self.usecols,
        }


class DatasetDesc(str, Enum):

    def __new__(cls, value: str, usecols: range):
        obj = str.__new__(cls)
        obj._value_ = value
        obj.usecols = usecols
        return obj

    DOUGLAS = 'dataset_douglas.zip', range(3, 5)
    USA_KENDRICK = 'dataset_usa_kendrick.zip', range(3, 5)
    USCB = 'dataset_uscb.zip', [0, 1, 3, 4, 5, 6, 9]

    def get_kwargs(self) -> dict[str, Any]:

        NAMES = [
            'source', 'table', 'note', 'group1', 'group2', 'group3', 'series_id'
        ]

        return {
            'filepath_or_buffer': DATA_DIR.joinpath(self.value),
            'header': 0,
            'names': (None, NAMES)[self.name in ['USCB']],
            'index_col': (None, 1)[self.name in ['DOUGLAS', 'USA_KENDRICK']],
            'skiprows': (0, 4)[self.name in ['USA_BROWN']],
            'usecols': self.usecols,
            'low_memory': (True, False)[self.name in ['USCB']],
        }


class URL(Enum):
    FIAS = 'https://apps.bea.gov/national/FixedAssets/Release/TXT/FixedAssets.txt'
    NIPA = 'https://apps.bea.gov/national/Release/TXT/NipaDataA.txt'

    def get_kwargs(self) -> dict[str, Any]:

        NAMES = ['series_ids', 'period', 'value']

        kwargs = {
            'header': 0,
            'names': NAMES,
            'index_col': 1,
            'thousands': ','
        }
        if requests.head(self.value).status_code == HTTPStatus.OK:
            kwargs['filepath_or_buffer'] = io.BytesIO(
                requests.get(self.value).content
            )
        else:
            kwargs['filepath_or_buffer'] = self.value.split('/')[-1]
        return kwargs


@dataclass(frozen=True, eq=True)
class SeriesID:
    series_id: str
    source: Union[Dataset, URL]
