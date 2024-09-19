import numpy as np
import pandas as pd
from datetime import timedelta
from dataclasses import dataclass


"""---------------------------------------------Aggregate Work-------------------------------------------------------"""
@dataclass
class AggHandler:
    dayslowoh: int
    smonthly_list: list
    file_output: str
    security: str
    timeframe: str
    algo_name: str
    combo: int
    total_combos: int

    def __init__(self, emac_setup, combo_start):
        self.trade_df = []
        self.paramsets = []
        self.param_names = []
        self.file_output = emac_setup.file_output
        self.security = emac_setup.security
        self.timeframe = emac_setup.timeframe
        self.algo_name = emac_setup.algo_name
        self.combo = combo_start
        self.total_combos = emac_setup.total_combos

    def period_save(self):
        print("Saving Data")
        param_df, trade_dfs = self.build_aggregated_data()
        self.save_aggregated_data(param_df, trade_dfs)
        self.paramsets = []
        self.trade_df = []

    def decide_save(self, n_dfs):
        if len(self.paramsets) > n_dfs:
            self.period_save()

    def build_aggregated_data(self):
        param_df = pd.DataFrame(self.paramsets, columns=[['paramset_id'] + self.param_names]).reset_index(drop=True)
        trade_dfs = pd.concat(self.trade_df).reset_index(drop=True)

        return param_df, trade_dfs

    def save_aggregated_data(self, param_df, trade_dfs):
        print(f"{self.security}_{self.timeframe}_{self.algo_name}_{self.combo}")
        param_save_file = \
            f'{self.file_output}\\{self.security}_{self.timeframe}_{self.algo_name}_{self.combo}_params.feather'
        trade_save_file= \
            f'{self.file_output}\\{self.security}_{self.timeframe}_{self.algo_name}_{self.combo}_trades.feather'

        param_df.to_feather(param_save_file)
        trade_dfs.to_feather(trade_save_file)


def adjust_dates(dates):
    try:
        date = pd.to_datetime(dates, format='%Y-%m-%d')
    except ValueError:
        try:
            date = pd.to_datetime(dates, format='%m/%d/%Y')
        except ValueError:
            date = pd.to_datetime(dates, format='%Y/%m/%d')
        
    return date


def adjust_datetime(datetimes):
    datetimes = pd.to_datetime(datetimes, format='%Y-%m-%d %H:%M:%S')
    return datetimes


def create_datetime(df):
    datetimes = pd.to_datetime(df['Date'] + ' ' + df['Time'], format='%m/%d/%Y %H:%M')

    return datetimes


def fix_tf_arrays(arr):
    arr = arr.astype(int)

    return arr


def subset_time(df, dbl_setup, subtract_time=0):
    """Subsets for start_time and end_time. Option to subtract an hour from start_time in order to allow some
    space for setups. This means that you don't have to run on the full dataset to get accurate info for the
    targetted time"""

    eod_mask = (df['DateTime'].dt.time >=
                (dbl_setup.start_time - timedelta(hours=subtract_time)).time()) & \
               (df['DateTime'].dt.time <= dbl_setup.eod_time.time())

    return df[eod_mask]


def filter_trades(df):
    df = df.loc[(df['bullTrade'] == 1) | (df['bearTrade'] == 1)]
    df = df[['DateTime', 'side', 'entryInd', 'entryPrice', 'exitInd', 'exitPrice']]
    df.reset_index(drop=True, inplace=True)

    return df


def get_side(df):
    side = np.where(df['bullTrade'] == 1, 'Bull', np.where(df['bearTrade'] == 1, 'Bear', ''))

    return side


def get_pnl(df):
    pnl = np.array(df['exitPrice'] - df['entryPrice'])
    bear_mask = df.loc[df['bearTrade'] == 1].index.to_list()
    pnl[bear_mask] = -pnl[bear_mask]

    return pnl


def max_drawdown(pnl):
    cumulative_max = np.maximum.accumulate(pnl)
    drawdown = cumulative_max - pnl
    max_draw = np.max(drawdown)

    return -max_draw


def analyze_params(pnl_df):
    month_df = (pnl_df.dropna(subset=['PnL']).groupby(['side', 'year', 'month'])
                .agg(cumPnl=('PnL', 'sum'),
                     maxDraw=('PnL', max_drawdown),
                     trades=('PnL', 'count')).reset_index())

    win_df = pnl_df[pnl_df['PnL'] > 0].groupby(['side', 'year', 'month']).size().reset_index(name='win_count')
    loss_df = pnl_df[pnl_df['PnL'] <= 0].groupby(['side', 'year', 'month']).size().reset_index(name='loss_count')
    combined_counts = pd.merge(win_df, loss_df, on=['side', 'year', 'month'], how='outer')
    combined_counts = combined_counts.fillna(0)
    month_df = pd.merge(month_df, combined_counts, on=['side', 'year', 'month'], how='left')
    month_df['win_percent'] = month_df['win_count'] / month_df['trades']

    return month_df


def reset_exit_entry(saved_df, working_df):
    """Moves trade logic from a previous point in the workflow to reset for new work
    :param saved_df: The df where the work is saved so that it can be reset after changes
    :param working_df: The df to be reset"""

    working_df['bullTrade'] = saved_df['bullTrade'].copy()
    working_df['bearTrade'] = saved_df['bearTrade'].copy()

    working_df['bullExit'] = saved_df['bullExit'].copy()
    working_df['bearExit'] = saved_df['bearExit'].copy()

    # if 'exitInd' in working_df.columns:
    #     working_df['exitInd'] = saved_df['exitInd'].copy()
    #     working_df['exitPrice'] = saved_df['exitPrice'].copy()


def keep_changes(saved_df, working_df):
    """Saves the current data so that it can be used to reset data later
    :param saved_df: The df where the work is saved so that it can be reset after changes
    :param working_df: The df to be reset"""

    saved_df['bullTrade'] = working_df['bullTrade'].copy()
    saved_df['bearTrade'] = working_df['bearTrade'].copy()

    saved_df['bullExit'] = working_df['bullExit'].copy()
    saved_df['bearExit'] = working_df['bearExit'].copy()

    # if 'exitInd' in working_df.columns:
    #     saved_df['exitInd'] = working_df['exitInd'].copy()
    #     saved_df['exitPrice'] = working_df['exitPrice'].copy()
