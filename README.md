# Brazilian Journal Preservation Gap

![Workflow](https://img.shields.io/badge/workflow-documented-blue)
![Validation](https://img.shields.io/badge/validation-partially%20manual-orange)

## Descrição

Este repositório contém os dados, scripts e pipeline necessários para reproduzir o estudo:

**"Desalinhamento entre produção editorial, indexação e preservação digital de periódicos brasileiros: evidências da Rede Cariniana (2000–2024)"**

O estudo investiga inconsistências estruturais entre:
- produção editorial
- indexação (DOAJ, Latindex, OpenAlex)
- preservação digital (Rede Cariniana / LOCKSS)

---

## Estrutura do projeto

```
project/
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

Instalação:

```bash
pip install pandas numpy
```

---

## Dados de entrada

Coloque os arquivos em:

```
data/raw/
```

Arquivos esperados:
- `openalex_BR_2000_2024_tratado.csv`
- `doaj_brasil_extraido_do_json.csv`
- `latindex-journals-brasileiros-v2.csv`
- `titledb.xml`

---

## Execução do pipeline

Execute a partir da raiz do projeto:

```bash
chmod +x run_pipeline.sh
./run_pipeline.sh
```

---

## Etapas do pipeline

1. Ingestão (LOCKSS / Cariniana)
2. Preparação de bases externas
3. Reconciliação de dados
4. Geração da base consolidada
5. Classificação
6. Validação manual

---

## Saídas principais

Localização:

```
data/processed/
```

Arquivos:
- `periodicos_enriquecido.csv`
- `diagnostico_risco.csv`
- `periodicos_base_corrigido_v3.csv`
- `auditoria_match.csv`
- `infraestrutura_url.csv`

---

## Validação manual

Inclui:
- amostragem estratificada
- verificação manual de URLs
- classificação final (confirmado, inconclusivo)

---

## Reprodutibilidade (IMPORTANTE)

Alguns scripts utilizam caminhos fixos internos como:

```
data/processed/...
```

Portanto:
- execute sempre a partir da raiz do repositório
- não altere a estrutura de diretórios

---

## Limitações

- inconsistências de ISSN
- dependência de disponibilidade de URLs
- necessidade de validação manual

---

## 🇺🇸 English Version

## Description

This repository provides the data, scripts, and pipeline required to reproduce the study:

**"Misalignment between editorial production, indexing, and digital preservation of Brazilian journals: evidence from the Cariniana Network (2000–2024)"**

The study analyzes inconsistencies between:
- editorial activity
- indexing systems (DOAJ, Latindex, OpenAlex)
- digital preservation (Cariniana / LOCKSS)

---

## Project structure

```
project/
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

## Requirements

- Python 3.9+
- pandas
- numpy

Install:

```bash
pip install pandas numpy
```

---

## Input data

Place files in:

```
data/raw/
```

Expected files:
- `openalex_BR_2000_2024_tratado.csv`
- `doaj_brasil_extraido_do_json.csv`
- `latindex-journals-brasileiros-v2.csv`
- `titledb.xml`

---

## Running the pipeline

Run from project root:

```bash
chmod +x run_pipeline.sh
./run_pipeline.sh
```

---

## Pipeline steps

1. Ingestion (LOCKSS / Cariniana)
2. External data preparation
3. Data reconciliation
4. Base generation
5. Classification
6. Manual validation

---

## Main outputs

Location:

```
data/processed/
```

Files:
- `periodicos_enriquecido.csv`
- `diagnostico_risco.csv`
- `periodicos_base_corrigido_v3.csv`
- `auditoria_match.csv`
- `infraestrutura_url.csv`

---

## Manual validation

Includes:
- stratified sampling
- manual URL validation
- final classification (confirmed, inconclusive)

---

## Reproducibility (IMPORTANT)

Some scripts use hardcoded internal paths:

```
data/processed/...
```

Therefore:
- run from repository root
- keep directory structure unchanged

---

## Limitations

- ISSN inconsistencies
- dependency on URL availability
- partial manual validation

---

## License

MIT
