#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug  8 22:40:39 2023

@author: green-machine
"""


from core.classes import Dataset, SeriesID
from core.funcs import stockpile_usa_hist

if __name__ == '__main__':

    SERIES_IDS = [SeriesID('J0149', Dataset.USCB)]

    df = stockpile_usa_hist(SERIES_IDS)
    print(df)

    df = df.iloc[:, :-1]
    print(df)
