import pandas as pd
import re


"""
Желаемы размер чанка определяется числами a и b
Если кол-во записей меньше чем a, то чанк продолжает заполняеться новыми значениями.

Если кол-во записей в чанке больше чем a, то нужно следить за изменениями поля dt:
    1. если значение dt поменялось до того как чанк заполнился до значения b, то эти записи остаются в чанке.
    2. если dt поменялось после того как кол-во записей стало больше b, то 
       записи с предпоследним dt идут в новый чанк (dt, который вылез за предел b).
       В предыдущем чанке записей будет в промежутке (a,b)

Если изменение dt было до достижения a,
следующее изменение dt было после достижения b, то размер чанка будет больше b.
"""

def test_asset():

    dfs = pd.date_range("2023-01-01 00:00:00", "2023-01-01 00:00:09", freq="s")

    return pd.DataFrame({"dt": dfs.repeat(2)})


def test_avg_chunck_size(chunk_list, new_chunk):
    new_chunk_size = new_chunk[1] - new_chunk[0]
    if len(chunk_list):
        avg_chunk_size = sum((map(lambda x: (x[1] - x[0]), chunk_list))) / len(chunk_list)
        if new_chunk_size > avg_chunk_size * 3:
            print("Data skew warning !")


def test_date_format(x):
    pattern = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"
    if not re.match(pattern, x):
        raise Exception("Not Valid format", x)
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

        test_date_format(row["dt"])

        v = row["dt"]
        if v == prev_dt:
            if flag == 1 and cnt > b:
                l_chunk.append((s, s2))
                cnt = x - s2
                s = s2 + 1
                flag = 0
        else:
            if cnt >= a:
                if cnt <= b:
                    flag = 1
                    s2 = x - 1
                else:
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

    df = pd.read_csv("test_6.csv")

    row_cnt, col_cnt = df.shape
    print("rows", row_cnt)

    a, b = 3, 5

    l = chunks(df, a, b)

    print(l)

    for a, b in l:
        print(f"{a}-{b}----------------------")
        df_i = df.iloc[a : b + 1]
        print(df_i)

        # nan_row = df_i.isnull().values.any()
        nan_row = df_i["atr1"].isnull()
        if nan_row.any():
            print("Null Values", df_i[nan_row])
            # raise Exception ('В столбце atr1 есть null значения')
        # df.to_csv(f'chunk-{a}-{b}.csv', index=False)
