# Lacuna de Preservação de Periódicos Brasileiros

![Fluxo de trabalho](https://img.shields.io/badge/workflow-documentado-blue)
![Validação](https://img.shields.io/badge/validação-parcialmente%20manual-orange)
[![DOI](https://zenodo.org/badge/10.5281/zenodo.19411254.svg)](https://doi.org/10.5281/zenodo.19411254)

---

## Descrição

Este repositório disponibiliza os dados, scripts e pipeline computacional necessários para reproduzir o estudo:

**"Desalinhamento entre produção editorial, indexação e preservação digital de periódicos brasileiros: evidências da Rede Cariniana (2000–2024)"**

O estudo investiga inconsistências estruturais entre:
- produção editorial
- indexação (DOAJ, Latindex, OpenAlex)
- preservação digital (Rede Cariniana / LOCKSS)

---

## Reprodutibilidade

Este repositório permite a reprodutibilidade completa das etapas computacionais e a reprodutibilidade parcial das etapas manuais.

### Etapas totalmente reprodutíveis

- Extração de dados do LOCKSS (`titledb.xml`)
- Integração com OpenAlex, DOAJ e Latindex
- Reconciliação e consolidação de periódicos
- Modelagem e classificação de risco
- Validação automatizada da infraestrutura de acesso

### Etapas parcialmente reprodutíveis

- Revisão manual de periódicos residuais
- Validação manual de URLs e atividade editorial

Todos os processos manuais seguem protocolos estruturados e documentados, e seus resultados estão incluídos no repositório.

> ⚠️ **Importante**
> Alguns scripts utilizam caminhos fixos internos (`data/processed/...`).
> Execute sempre a partir da raiz do repositório e não altere a estrutura de diretórios.

---

## Estrutura do projeto

```
projeto/
│
├── data/
│   ├── raw/
│   └── processed/
│
├── scripts/
│   ├── 01_ingestion/
│   ├── 02_reconciliation/
│   ├── 03_base_generation/
│   ├── 04_classification/
│   └── 05_validation/
│
├── outputs/
│
├── run_pipeline.sh
└── README.md
```

---

## Requisitos

- Python 3.9+
- pandas
- numpy
- requests

Instalação:

```bash
pip install pandas numpy requests
```

---

## Dados de entrada

Coloque os arquivos em:

```
data/raw/
```

Arquivos esperados:

- `titledb.xml`
- `openalex_BR_2000_2024_tratado.csv`
- `doaj_brasil_extraido_do_json.csv`
- `latindex-journals-brasileiros-v2.csv`

---

## Visão geral do pipeline

O pipeline é composto por três camadas integradas:

**1. Integração de dados**
- Extração da base LOCKSS (Cariniana)
- Integração com OpenAlex, DOAJ e Latindex

**2. Modelagem de risco**
- Análise da atividade editorial
- Verificação de indexação
- Avaliação da cobertura de preservação
- Análise da infraestrutura de acesso

**3. Validação**
- Revisão manual de casos residuais
- Validação automatizada de URLs
- Validação manual (amostra estratificada)

---

## Execução do pipeline

Execute a partir da raiz do projeto:

```bash
chmod +x run_pipeline.sh
TITLEDB_XML=./data/raw/titledb.xml ./run_pipeline.sh
```

---

## Etapas detalhadas do pipeline

1. **Ingestão** — parse do `titledb.xml`; geração de `preservacao_titledb.csv` e `periodicos_base.csv`
2. **Preparação de bases externas** — normalização das bases OpenAlex, DOAJ e Latindex
3. **Reconciliação** — correspondência entre bases, tratamento de inconsistências de ISSN e geração de base enriquecida
4. **Geração da base consolidada** — consolidação em nível de periódico
5. **Classificação** — cálculo de trajetória editorial e classificação de risco
6. **Validação** — aplicação de validações automatizadas e manuais

---

## Estrutura de validação

A validação combina procedimentos automatizados e manuais para garantir robustez analítica.

### 1. Revisão de periódicos residuais (manual)

Casos ambíguos são revisados manualmente.

- **Dataset:** `revisao_manual_periodicos_residuais.csv`
- **Decisões possíveis:** `manter`, `fundir`
- **Agrupamento:** via campo `grupo_fusao`

```bash
python aplicar_revisao_periodicos_residuais.py \
  --periodicos data/processed/periodicos_base_corrigido_v3.csv \
  --revisao data/processed/revisao_manual_periodicos_residuais.csv \
  --outdir data/processed
```

Saídas:
- `periodicos_base_revisado.csv`
- `mapa_revisao_periodicos_residuais.csv`

---

### 2. Validação da infraestrutura de acesso (automática)

Realizada com `validador_urls_cariniana.py`. Gera:
- `infraestrutura_url.csv`
- `resumo_validacao_urls.csv`

**Modo preferencial** (pós-reconciliação): usa `periodicos_base.csv`, que reflete o conjunto de dados consolidado e captura URLs atualizadas.

**Modo alternativo** (pré-reconciliação): usa `preservacao_titledb.csv`, que permite execução antecipada, mas pode não refletir mudanças recentes de URLs.

```bash
# Modo preferencial
python validador_urls_cariniana.py data/processed/periodicos_base.csv --outdir data/processed

# Modo alternativo
python validador_urls_cariniana.py data/processed/preservacao_titledb.csv \
  --source-system titledb \
  --outdir data/processed
```

---

### 3. Validação manual (URLs e atividade editorial)

Uma amostra estratificada é avaliada manualmente. Inclui:
- verificação de URL
- análise de redirecionamento
- inspeção de conteúdo
- confirmação de atividade editorial

**Saída:** `validation_manual.csv`

Esta etapa permite validar empiricamente os processos automatizados, medir a complexidade de recuperação e identificar padrões de falha de infraestrutura.

---

## Principais saídas

Localizadas em `data/processed/`:

- `periodicos_enriquecido.csv`
- `diagnostico_risco.csv`
- `periodicos_base_corrigido_v3.csv`
- `auditoria_match.csv`
- `infraestrutura_url.csv`

---
## Dicionário de dados — validação manual

O arquivo `validation_manual.csv` contém os resultados estruturados da validação manual aplicada a uma amostra estratificada de periódicos.

### Identificação
- `periodico_id`: identificador único do periódico na base do estudo.
- `titulo_principal`: título principal do periódico utilizado na validação.
- `issn_impresso`: ISSN da versão impressa, quando disponível.
- `issn_eletronico`: ISSN da versão eletrônica, quando disponível.

### Contexto
- `grupo_validacao`: grupo da amostra estratificada (`A` ou `B`).
- `fonte_consultada`: fonte inicial usada como referência para localizar o periódico.

### URL
- `url_funcional_identificada`: URL inicialmente associada ao periódico na base.
- `url_real_correta_identificada`: indica se a URL correta do periódico foi identificada durante a validação (`True` ou `False`).

### Atividade editorial
- `periodico_ativo_manual`: indica se o periódico apresenta evidência de atividade editorial no momento da validação manual (`True` ou `False`).
- `ultimo_ano_editorial_manual`: ano mais recente de publicação identificado manualmente.
- `evidencia_volume_recente`: indica a presença de volume, fascículo ou conteúdo editorial recente (`True` ou `False`).

### Infraestrutura
- `mudanca_url_observada`: indica se foi observada mudança de URL ou de domínio em relação ao registro originalmente consultado (`True` ou `False`).
- `redirecionamento_observado`: indica se houve redirecionamento ao acessar a URL original (`True` ou `False`).
- `problema_infraestrutura`: tipo principal de problema identificado na infraestrutura de acesso. Valores possíveis:
  - `erro_http`: erro HTTP ao acessar a URL.
  - `redirect`: redirecionamento problemático ou incorreto.
  - `manutencao`: página ou sistema em manutenção.
  - `colecao_em_vez_de_periodico`: a URL leva a um portal, coleção ou lista de periódicos, e não diretamente à página do periódico.
  - `sem_falha_tecnica_especifica`: não foi identificado problema técnico específico, embora a recuperação tenha exigido navegação adicional.

### Correspondência
- `correspondencia_periodico`: grau de correspondência entre o conteúdo encontrado e o periódico esperado. Valores:
  - `correto`: o periódico localizado corresponde ao periódico esperado.
  - `duvidoso`: a correspondência não pôde ser confirmada com segurança.
- `inconsistencia_issn`: indica se foi observada inconsistência entre ISSN, eISSN, título ou metadados associados (`True` ou `False`).

### Qualidade da URL
- `qualidade_url_origem`: classificação funcional da URL originalmente consultada. Valores:
  - `direta`: a URL leva diretamente à página do periódico.
  - `indireta`: a URL leva a uma página intermediária, como portal institucional, coleção ou lista de periódicos.
  - `invalida`: a URL não funciona adequadamente para acesso ao periódico.

### Resolução
- `resolucao_direta_real_identificada`: indica se a URL originalmente consultada resolve diretamente para o periódico correto, sem necessidade de busca complementar (`True` ou `False`).

### Recuperação
- `recuperacao_assistida`: nível de esforço necessário para localizar a URL correta do periódico. Valores:
  - `baixa_complexidade`: recuperação simples, com navegação trivial.
  - `media_complexidade`: recuperação possível com navegação em portal, coleção ou busca interna.
  - `alta_complexidade`: recuperação dependente de busca externa, inferência manual ou múltiplas tentativas.

### Resultado
- `observacao_validacao`: campo livre para registrar observações relevantes.
- `classificacao_validacao`: resultado final da validação. Valores:
  - `confirmado`: a identificação do periódico e da URL correta pôde ser confirmada com segurança.
  - `inconclusivo`: a validação não pôde ser concluída com confiança suficiente.

## Observações metodológicas

- As definições operacionais foram aplicadas de forma consistente em toda a amostra validada manualmente.
- A validação manual combina verificação de URL, inspeção de conteúdo e conferência de consistência de metadados.
- O objetivo da base é apoiar tanto análises quantitativas quanto a interpretação qualitativa dos padrões de desalinhamento e falha de infraestrutura.
---

## Análise e reprodução dos resultados
 
Além do pipeline principal de processamento de dados, este repositório inclui um script analítico responsável por reproduzir os resultados centrais apresentados no estudo.
 
O script realiza:
- análise da distribuição dos resultados da validação manual
- avaliação da qualidade das URLs
- análise da complexidade de recuperação
- cruzamentos entre variáveis
- verificações de consistência interna
- integração com a base de diagnóstico de risco
 
**Localização do script:** `scripts/06_analysis/analysis.py`
 
### Execução
 
A partir da raiz do repositório:
 
```bash
python3 scripts/06_analysis/analysis.py \
  --data-dir data/processed \
  --outdir outputs/tables
```
 
### Saídas
 
O script gera tabelas de resumo em `outputs/tables/`, incluindo:
 
- distribuição dos resultados de validação
- distribuição da qualidade das URLs
- distribuição da complexidade de recuperação
- cruzamentos (ex.: qualidade da URL × resultado da validação)
- problemas de infraestrutura × resultado
- complexidade de recuperação × resultado
- variáveis de risco × resultado
 
### Verificações analíticas
 
O script também executa verificações de consistência interna para validar a confiabilidade do processo de validação manual. Espera-se que:
 
- nenhum caso classificado como `confirmado` apresente URL inválida
- nenhum caso classificado como `inconclusivo` apresente URL corretamente resolvida
 
Essas verificações garantem coerência entre a classificação atribuída e as condições observadas de acesso.
 
### Nota de interpretação
 
Os resultados analíticos produzidos por este script sustentam os principais achados do estudo, especialmente:
 
- a predominância de padrões de acesso indireto
- a forte associação entre URLs inválidas e casos inconclusivos
- o papel da complexidade de recuperação como determinante da resolubilidade
 
> **Observação importante**
> Este script não é apenas complementar: ele permite reproduzir os resultados quantitativos apresentados na seção de resultados do artigo, constituindo parte essencial da reprodutibilidade do estudo.

## Limitações

- Inconsistências de ISSN entre bases
- Dependência da disponibilidade das URLs
- Necessidade de validação manual em casos residuais
- Cobertura incompleta do OpenAlex como proxy editorial

---

## Citação

O repositório é versionado e arquivado via Zenodo, com DOI atribuído a cada release, permitindo citação formal.

---

## Licença

MIT
