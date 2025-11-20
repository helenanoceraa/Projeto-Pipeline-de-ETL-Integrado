# 🌳 Pipeline de ETL — Análise de Desmatamento na Amazônia

Este repositório apresenta o desenvolvimento de um **pipeline ETL (Extract, Transform, Load)** focado em integrar, tratar e disponibilizar dados relacionados ao desmatamento e à degradação ambiental na Amazônia para análises e dashboards.

---

## 📖 Visão Geral

### 🎯 Problema de Negócio

O desmatamento na Amazônia Legal é um dos maiores desafios socioambientais do Brasil. Ele impacta diretamente:

* o clima e a biodiversidade
* o ciclo hidrológico
* a qualidade de vida das populações locais
* emissões de CO₂
* políticas públicas e indicadores ESG

Compreender **como**, **onde** e **em que ritmo** o desmatamento ocorre é essencial para gerar insights estratégicos, monitoramento ambiental e tomada de decisão.

### 🚀 Objetivo do Projeto

Construir e automatizar um **pipeline de dados completo**, capaz de:

* extrair dados brutos (Bronze)
* limpar, padronizar e consolidar informações (Silver)
* estruturar um Data Warehouse com tabelas dimensionais e fato (Gold)

O resultado final pode ser consumido por ferramentas como **Power BI**, gerando análises consistentes e confiáveis para estudos ambientais, políticas públicas ou relatórios corporativos.

---

## ⚙️ Instalação e Configuração

### 🔧 Pré-requisitos

* **Python 3.8+**
* **DBeaver** (ou qualquer cliente SQL)
* Dados brutos já incluídos no repositório na pasta `data/bronze/`

### 📦 Passo a Passo

1. **Clone o repositório**

```bash
git clone https://github.com/helenanoceraa/Projeto-Pipeline-de-ETL-Integrado
cd Projeto-Pipeline-de-ETL-Integrado
```

2. **Crie e ative o ambiente virtual**

```bash
# Windows
python -m venv venv
.\venv\Scripts\activate

# macOS / Linux
python3 -m venv venv
source venv/bin/activate
```

3. **Instale as dependências**

```bash
pip install -r requirements.txt
```

---

## ▶️ Execução do Pipeline

### **1️⃣ Criar a Camada Silver (limpeza e transformação)**

O script abaixo lê os dados brutos da pasta `bronze`, executa as transformações e gera o arquivo `deforestation_silver_layer.csv`.

> Também é possível visualizar o notebook usado na criação inicial do pipeline:
> [https://colab.research.google.com/drive/1uTfQqEzOkbX70TZC4J7AnVvRgYlmE1AK?usp=sharing](https://colab.research.google.com/drive/1uTfQqEzOkbX70TZC4J7AnVvRgYlmE1AK?usp=sharing)

```bash
python src/python/extract.py
```

---

### **2️⃣ Conectar ao Banco SQLite**

Abra o **DBeaver** e conecte-se ao arquivo:

```
db/desmatamento.db
```

Ele estará inicialmente vazio — o restante do pipeline irá preenchê-lo.

---

### **3️⃣ Executar o Pipeline de Carga (Data Warehouse)**

Este script cria e popula:

* `DimTempo`
* `DimLocalidade`
* `FatoDesmatamento`

```bash
python src/pipeline/run_pipeline.py
```

As tabelas aparecerão populadas no DBeaver após a execução.

---

### **4️⃣ Criar View Agregada (Camada Gold)**

Cria a view `vw_desmatamento_agregado`, usada diretamente no BI.

```bash
python src/pipeline/create_views.py
```

---

### **5️⃣ Validar a Camada Gold**

Verifica estrutura, criação e existência de dados na view.

```bash
python src/pipeline/validate_gold_layer.py
```

---

### **6️⃣ (Opcional) Criar a Camada Gold em Arquivo CSV**

Gera o arquivo:

```
data/gold/desmatamento_por_ano_estado.csv
```

```bash
python src/pipeline/create_gold_layer.py
```

---

## 📊 Fontes de Dados

Os dados utilizados provêm do **INPE | Terra Brasilis**, incluindo:

### 🔹 PRODES — Dados completos em formato GeoPackage

> *Quanto foi desmatado por ano na Amazônia Legal.*

### 🔹 Shapefile — Incremento anual do desmatamento

> *Avisos de desmatamento contendo estado, data e área desmatada.*

### 🔹 Taxas anuais de desmatamento — Amazônia Legal

> *Medições acumuladas por ano e indicadores ambientais relacionados.*
