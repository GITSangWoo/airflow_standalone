import pandas as pd
import sys 

def make_parquet(csvpath=sys.argv[1],savepath=sys.argv[2]):
    print(csvpath)
    print(savepath)
    df = pd.read_csv(csvpath, on_bad_lines='skip', encoding = "latin",names=['dt', 'cmd', 'cnt'])
    df['dt'] = df['dt'].str.replace('^', '')
    df['cmd'] = df['cmd'].str.replace('^', '')
    df['cnt'] = df['cnt'].str.replace('^', '')
    df['cnt'] = df['cnt'].astype(int)
    df.to_parquet(savepath)

