import pandas as pd

import numpy as np


def make_timeseries(
    start="2023-01-01 00:00:00", end="2023-01-01 00:00:15", freq="1s", seed=None
):
    index = pd.date_range(start=start, end=end, freq=freq, name="dt")
    n = len(index)
    state = np.random.RandomState(seed)
    columns = {
        "atr1": state.choice(["A", "B", "C"], size=n),
        "atr2": state.poisson(1000, size=n),
        "atr3": state.rand(n) * 2 - 1,
    }
    df = pd.DataFrame(columns, index=index, columns=sorted(columns))
    if df.index[-1] == end:
        df = df.iloc[:-1]
    return df


def prepare_data():
    timeseries = [
        make_timeseries(freq="1min", seed=i).rename(columns=lambda x: f"{x}_{i}")
        for i in range(10)
    ]
    ts_wide = pd.concat(timeseries, axis=1)
    ts_wide.to_csv("csv_chunk.csv", index=False)


def test_asset():

    dfs = pd.date_range("2023-01-01 00:00:00", "2023-01-01 00:00:05", freq="s")
    df = pd.DataFrame({"dt": dfs.repeat(3)})

    return df


"""
Желаемый размер чанка определяется значением b
"""

if __name__ == "__main__":

    df = make_timeseries(
        start="2023-01-01 00:00:00", end="2023-01-01 00:00:15", freq="1s", seed=None
    )
    df.to_csv("text_6.csv")

    exit()

    df = test_asset()
    # df = pd.read_csv('csv_chunk.csv')

    row_cnt, col_cnt = df.shape
    print("rows", row_cnt)

    a, b = 1, 7

    prev_dt = None
    s = 0
    cnt = 0
    l = []
    for x in range(0, row_cnt):

        if x == 6:
            pass
        row = df.iloc[x]
        v = row["dt"]
        if v == prev_dt:
            cnt += 1
        else:
            if cnt < b:
                cnt += 1
            else:
                l.append((s, x - 1))
                s = x
                cnt = 1
        prev_dt = v

    if row_cnt - s > 0:
        l.append((s, row_cnt))

    print(l)

    for a, b in l:
        print(f"{a}-{b}----------------------")
        print(df.iloc[a : b + 1])
