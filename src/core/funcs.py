#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug  8 22:43:31 2023

@author: green-machine
"""


from functools import cache

import pandas as pd
from core.classes import SeriesID
from pandas import DataFrame


@cache
def read_usa_hist(series_id: SeriesID) -> DataFrame:
    """
    Retrieves Data from Enumerated Historical Datasets
    Parameters
    ----------
    series_id : Series ID

    Returns
    -------
    DataFrame
        ================== =================================
        df.index           Period
        df.iloc[:, 0]      Series IDs
        df.iloc[:, 1]      Values
        ================== =================================
    """

    return pd.read_csv(**series_id.datasource.get_kwargs())


def pull_by_series_id(df: DataFrame, series_id: str) -> DataFrame:
    """


    Parameters
    ----------
    df : DataFrame
        ================== =================================
        df.index           Period
        df.iloc[:, 0]      Series IDs
        df.iloc[:, 1]      Values
        ================== =================================
    series_id : str

    Returns
    -------
    DataFrame
        ================== =================================
        df.index           Period
        df.iloc[:, 0]      Series
        ================== =================================
    """
    assert df.shape[1] == 2
    return df[df.iloc[:, 0] == series_id].iloc[:, [1]].rename(
        columns={"value": series_id}
    )


def stockpile_usa_hist(series_ids: list[SeriesID]) -> DataFrame:
    """


    Parameters
    ----------
    series_ids : list[SeriesID]
        DESCRIPTION.

    Returns
    -------
    DataFrame
        ================== =================================
        df.index           Period
        ...                ...
        df.iloc[:, -1]     Values
        ================== =================================

    """
    return pd.concat(
        map(
            lambda _: read_usa_hist(_).pipe(pull_by_series_id, _.series_id),
            series_ids
        ),
        axis=1,
        sort=True
    )
