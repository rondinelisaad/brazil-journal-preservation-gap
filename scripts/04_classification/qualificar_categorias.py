import pandas as pd
import re

df = pd.read_csv('data/processed/preservacao_titledb.csv')

# chave ISSN combinada
df['chave_issn'] = df['issn_titledb_norm'].fillna('') + '|' + df['eissn_titledb_norm'].fillna('')

# apenas conflitos (mais de um título por chave)
conf = df.groupby('chave_issn')['journal_title_titledb'].nunique()
conf = conf[conf > 1]

df_conf = df[df['chave_issn'].isin(conf.index)].copy()

# normalização simples de título
def norm_title(x):
    if pd.isna(x):
        return ""
    x = x.lower()
    x = re.sub(r'[^a-z0-9 ]', '', x)
    return x.strip()

df_conf['title_norm'] = df_conf['journal_title_titledb'].apply(norm_title)

# agrupamento por chave
rows = []

for chave, g in df_conf.groupby('chave_issn'):
    titles = g['journal_title_titledb'].dropna().unique().tolist()
    titles_norm = g['title_norm'].dropna().unique().tolist()

    # remove vazios
    titles_norm = [t for t in titles_norm if t.strip()]

    n = len(titles_norm)

    if n <= 1:
        categoria = "inconclusivo"

    elif n == 2:
        t1, t2 = titles_norm

        if len(t1) < 6 or len(t2) < 6:
            categoria = "alias_abreviacao"

        elif t1 in t2 or t2 in t1:
            categoria = "variacao_titulo"

        elif ("revista" in t1 and "journal" in t2) or ("journal" in t1 and "revista" in t2):
            categoria = "idioma"

        else:
            categoria = "possivel_colisao"

    else:
        categoria = "multiplo_conflito"

    rows.append({
        "chave_issn": chave,
        "qtd_titulos": len(titles),
        "categoria": categoria
    })

res = pd.DataFrame(rows)

# tabela final
tabela = res['categoria'].value_counts().reset_index()
tabela.columns = ['categoria', 'quantidade']

# percentual
tabela['percentual'] = (tabela['quantidade'] / tabela['quantidade'].sum()) * 100

print(tabela)
