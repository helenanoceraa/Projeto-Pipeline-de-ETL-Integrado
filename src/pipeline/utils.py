# Funções utilitárias para a pipeline de dados.
# Centraliza operações comuns para todos os scripts.

import sqlite3
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime


def configurar_logs(caminho_log='logs/pipeline_run.log'):
    """
    Configura o sistema de logs para registrar todas as operações

    Args:
        caminho_log: Caminho onde o arquivo de log será salvo
    """
    # Cria a pasta logs se não existir
    Path(caminho_log).parent.mkdir(parents=True, exist_ok=True)

    # Configuração do logging
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(caminho_log, encoding='utf-8'),
            logging.StreamHandler()  # Também mostra no console
        ]
    )

    return logging.getLogger(__name__)


def conectar_banco(caminho_db='db/desmatamento.db'):
    """
    Conecta ao banco de dados SQLite
    Cria o banco e a pasta se não existirem

    Args:
        caminho_db: Caminho para o arquivo do banco SQLite

    Returns:
        Conexão com o banco de dados
    """
    # Cria a pasta db se não existir
    Path(caminho_db).parent.mkdir(parents=True, exist_ok=True)

    # Conecta ao banco (cria se não existir)
    conexao = sqlite3.connect(caminho_db)

    return conexao


def ler_camada_silver(caminho_csv='data/silver/deforestation_silver_layer.csv'):
    """
    Lê o arquivo CSV da camada Silver

    Args:
        caminho_csv: Caminho para o arquivo CSV

    Returns:
        DataFrame do pandas com os dados
    """
    try:
        df = pd.read_csv(caminho_csv)
        logging.info(f"✅ Arquivo Silver lido com sucesso: {len(df)} registros")
        return df
    except FileNotFoundError:
        logging.error(f"❌ Arquivo não encontrado: {caminho_csv}")
        raise
    except Exception as e:
        logging.error(f"❌ Erro ao ler arquivo: {str(e)}")
        raise


def criar_tabelas(conexao):
    """
    Cria as tabelas do Data Warehouse se não existirem

    Args:
        conexao: Conexão com o banco SQLite
    """
    cursor = conexao.cursor()

    # Tabela DimTempo
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS DimTempo (
            id_tempo INTEGER PRIMARY KEY AUTOINCREMENT,
            data_completa DATE UNIQUE NOT NULL,
            ano INTEGER NOT NULL,
            mes INTEGER NOT NULL,
            dia INTEGER NOT NULL,
            ano_mes TEXT NOT NULL,
            semestre INTEGER NOT NULL
        )
    """)

    # Tabela DimLocalidade
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS DimLocalidade (
            id_localidade INTEGER PRIMARY KEY AUTOINCREMENT,
            estado TEXT UNIQUE NOT NULL,
            regiao TEXT NOT NULL
        )
    """)

    # Tabela FatoDesmatamento
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS FatoDesmatamento (
            id_fato INTEGER PRIMARY KEY AUTOINCREMENT,
            id_tempo INTEGER NOT NULL,
            id_localidade INTEGER NOT NULL,
            tipo_degradacao TEXT NOT NULL,
            area_km REAL NOT NULL,
            FOREIGN KEY (id_tempo) REFERENCES DimTempo(id_tempo),
            FOREIGN KEY (id_localidade) REFERENCES DimLocalidade(id_localidade)
        )
    """)

    conexao.commit()
    logging.info("✅ Tabelas criadas/verificadas com sucesso")


def obter_regiao_por_estado(estado):
    """
    Retorna a região do Brasil baseada no estado

    Args:
        estado: Sigla do estado (ex: AM, PA, RO)

    Returns:
        Nome da região
    """
    mapa_regioes = {
        'AC': 'Norte', 'AP': 'Norte', 'AM': 'Norte', 'PA': 'Norte',
        'RO': 'Norte', 'RR': 'Norte', 'TO': 'Norte',
        'AL': 'Nordeste', 'BA': 'Nordeste', 'CE': 'Nordeste',
        'MA': 'Nordeste', 'PB': 'Nordeste', 'PE': 'Nordeste',
        'PI': 'Nordeste', 'RN': 'Nordeste', 'SE': 'Nordeste',
        'DF': 'Centro-Oeste', 'GO': 'Centro-Oeste', 'MT': 'Centro-Oeste', 'MS': 'Centro-Oeste',
        'ES': 'Sudeste', 'MG': 'Sudeste', 'RJ': 'Sudeste', 'SP': 'Sudeste',
        'PR': 'Sul', 'RS': 'Sul', 'SC': 'Sul'
    }

    return mapa_regioes.get(estado, 'Não Identificado')


def contar_registros_tabela(conexao, nome_tabela):
    """
    Conta quantos registros existem em uma tabela

    Args:
        conexao: Conexão com o banco
        nome_tabela: Nome da tabela

    Returns:
        Número de registros
    """
    cursor = conexao.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {nome_tabela}")
    quantidade = cursor.fetchone()[0]

    return quantidade