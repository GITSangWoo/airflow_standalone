import pandas as pd 
import sys 

csvpath=sys.argv[1]
savepath=sys.argv[2]
df = pd.read_csv(csvpath, on_bad_lines='skip', encoding = "latin",names=['dt', 'cmd', 'cnt'])
df['dt'] = df['dt'].str.replace('^', '')
df['cmd'] = df['cmd'].str.replace('^', '')
df['cnt'] = df['cnt'].str.replace('^', '')
# 'coerce'는 변환할 수 없는 데이터를 만나면  그 값을 강제로 NaN으로 바꾸는것 
df['cnt'] = pd.to_numeric(df['cnt'],errors='coerce')
# NaN 값을 원하는 방식으로 처리한다 
df['cnt'] = df['cnt'].fillna(0).astype(int)
df['cnt'] = df['cnt'].astype(int)

df.to_parquet(f'{savepath}',partition_cols=['dt'])


