import pandas as pd
from utils.loaders import carregar_ml_full

DF_ML_FULL = carregar_ml_full()
# find some names with comments
with_comments = DF_ML_FULL[DF_ML_FULL['Comentário'].notna() & (DF_ML_FULL['Comentário'].str.strip()!='')]
print('total with comments', len(with_comments))
print(with_comments[['Nome','Comentário']].head(10))
