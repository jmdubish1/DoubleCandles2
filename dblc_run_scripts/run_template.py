import dblc_algo_logic.dblc_make_trades as dmt

setup_params = {
    'security': 'NQ',
    'timeframe': '15min',
    'time_length': '13years',
    'begin_date': '2014/01/01',
    'end_date': '2024/05/01',
    'use_end_date': True,
    'start_time': '08:00',
    'tick_size': .25
}

param_run_dict = {
    'lookbacks': range(3, 6),
    'fastEmaLens': range(10, 21, 2),
    'minCndlSizes': range(1, 22, 3),
    'finalCndlSizes': range(1, 22, 4),
    'finalCndlRatios': range(30, 71, 10),
    'stopLossPercents': range(10, 31, 5),
    'takeProfitPercents': range(30, 71, 10)
}

combo_start = 1

def main():
    dmt.make_trades(setup_params, param_run_dict, combo_start)

if __name__ == '__main__':
    main()
