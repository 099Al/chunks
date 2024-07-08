import pandas as pd
import re
import numpy as np


def test_asset():

    dfs = pd.date_range("2023-01-01 00:00:00", "2023-01-01 00:00:09", freq="s")

    return pd.DataFrame({"dt": dfs.repeat(2)})



def test_avg_chunck_size(chunk_list, new_chunk):
    new_chunk_size = new_chunk[1] - new_chunk[0]
    avg_chunk_size = sum((map(lambda x: (x[1] - x[0]), chunk_list))) / len(chunk_list)
    if new_chunk_size > avg_chunk_size * 3:
        print('Data skew warning !')

def test_date_format(x):
    pattern = r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
    if not re.match(pattern, x):
        raise Exception ('Not Valid format', x)
    return True


def chunks(df, a, b):
    prev_dt = None
    s = 0
    cnt = 0

    flag = 0
    s2 = 0

    l_chunk = []
    for x in range(0, row_cnt):
        cnt += 1
        row = df.iloc[x]

        test_date_format(row['dt'])

        v = row["dt"]
        if v == prev_dt:
            if flag == 1 and cnt > b:  # проверяем, чтобы не было переполнения
                # произошел перебор по одинаковой дате
                l_chunk.append((s, s2))  # закрыли предыдущий период
                cnt = x - s2
                s = s2 + 1  # обновили начало
                flag = 0
        else:
            # if (cnt < a):  # Продолжаем
            if cnt >= a:
                if cnt <= b:  # находимся внутри a b, произошла перемена дат
                    flag = 1
                    s2 = x - 1  # Предедущий dt с другим значением
                else:  # вышли за границу b => записываем чанк, и переобозначаем начало следующего
                    l_chunk.append((s, x - 1))
                    test_avg_chunck_size(l_chunk, (s, x - 1))
                    s = x
                    cnt = 0

        prev_dt = v





    if row_cnt - s > 0:
        l_chunk.append((s, row_cnt))

    return l_chunk


if __name__ == "__main__":

    # df = test_asset()

    df = pd.read_csv("test_3.csv")

    row_cnt, col_cnt = df.shape
    print("rows", row_cnt)

    a, b = 6, 8

    l = chunks(df, a, b)

    print(l)

    for a, b in l:
        print(f"{a}-{b}----------------------")
        print(df.iloc[a:b + 1])








