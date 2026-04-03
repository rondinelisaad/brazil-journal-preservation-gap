import pandas as pd
import re
from difflib import SequenceMatcher

df = pd.read_csv('data/processed/preservacao_titledb.csv')

df['chave_issn'] = df['issn_titledb_norm'].fillna('') + '|' + df['eissn_titledb_norm'].fillna('')

conf = df.groupby('chave_issn')['journal_title_titledb'].nunique()
conf = conf[conf > 1]

df_conf = df[df['chave_issn'].isin(conf.index)].copy()

def norm(x):
    if pd.isna(x):
        return ""
    x = x.lower()
    x = re.sub(r'[^a-z0-9 ]', '', x)
    return x.strip()

df_conf['title_norm'] = df_conf['journal_title_titledb'].apply(norm)

def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

rows = []

for chave, g in df_conf.groupby('chave_issn'):
    titles = g['journal_title_titledb'].dropna().unique().tolist()
    titles_norm = g['title_norm'].dropna().unique().tolist()
    titles_norm = [t for t in titles_norm if t]

    n = len(titles_norm)

    if n <= 1:
        categoria = "inconclusivo"

    elif n == 2:
        t1, t2 = titles_norm

        sim = similar(t1, t2)

        # 🔹 SIGLA (ex: REQ vs nome completo)
        if len(t1) <= 5 or len(t2) <= 5:
            categoria = "alias_abreviacao"

        # 🔹 MUITO parecido (variação)
        elif sim > 0.8:
            categoria = "variacao_titulo"

        # 🔹 idioma
        elif ("revista" in t1 and "journal" in t2) or ("journal" in t1 and "revista" in t2):
            categoria = "idioma"

        # 🔴 colisão mais provável
        elif sim < 0.4:
            categoria = "colisao_forte"

        # 🟡 zona cinza
        else:
            categoria = "colisao_possivel"

    else:
        categoria = "multiplo_conflito"

    rows.append({
        "chave_issn": chave,
        "qtd_titulos": len(titles),
        "categoria": categoria
    })

res = pd.DataFrame(rows)

tabela = res['categoria'].value_counts().reset_index()
tabela.columns = ['categoria', 'quantidade']
tabela['percentual'] = (tabela['quantidade'] / tabela['quantidade'].sum()) * 100

print(tabela)
