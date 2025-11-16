# Projeto Pipeline de ETL: Análise de Desmatamento e Degradação na Amazônia

Este repositório contém o desenvolvimento de um pipeline de ETL (Extract, Transform, Load) integrado, focado na coleta, processamento e disponibilização de dados para análise do desmatamento e da degradação ambiental na Amazônia.

---

## 🎯 Problema

O desmatamento da Amazônia Legal é um dos principais desafios socioambientais do Brasil.
Além do impacto climático e na biodiversidade, a degradação ambiental afeta a saúde das populações locais, o ciclo hidrológico e as emissões de CO₂, gerando implicações diretas para políticas públicas e para os indicadores ESG de grandes organizações.
O desafio é compreender como o desmatamento evolui ao longo do tempo, identificar as regiões mais críticas, e avaliar a relação entre pressão econômica, população e impacto ambiental.

---

## 🚀 Objetivo

Construir um pipeline ETL automatizado que extraia, transforme e carregue dados históricos de desmatamento na Amazônia, consolidando indicadores que apoiem monitoramento, tomada de decisão e políticas sustentáveis.
O projeto fornecerá uma visualização interativa no Power BI conectada ao banco de dados tratado, permitindo explorar padrões espaciais e temporais do desmatamento.

---

## 📊 Fontes de Dados

O pipeline processa dados das seguintes fontes:

* **INPE|Terra Brasilis: PRODES completo em formato vetorial - GeoPackage**
    * **Descrição:** Quanto que a Amazonia foi desmatada por ano
      
* **INPE|Terra Brasilis: Incremento anual no desmatamento - Shapelife**
    * **Descrição:** Avisos de desmatamento com o estado, data e área desmatada. 

* **INPE|Terra Brasilis: Taxas de desmatamento acumulada por ano - Amazonia Legal**
    * **Descrição:** Área queimada na Amazonia legal por mês


---
