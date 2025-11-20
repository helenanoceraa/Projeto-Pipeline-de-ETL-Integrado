# Script para validar a camada Gold (Views e arquivos CSV).
# Pode ser executado de forma independente após a criação da camada Gold.

import logging
import pandas as pd
from pathlib import Path
import sys

# Importa utilitários compartilhados
from utils import conectar_banco, configurar_logs

# --- Construção de Caminhos Absolutos ---
# Define o caminho raiz do projeto (a pasta que contém 'src', 'data', etc.)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_DB_PATH = PROJECT_ROOT / 'db' / 'desmatamento.db'
GOLD_DATA_PATH = PROJECT_ROOT / 'data' / 'gold'


def validar_camada_gold(caminho_db=DEFAULT_DB_PATH, caminho_gold=GOLD_DATA_PATH):
    """
    Valida os artefatos da camada Gold (Views no banco e arquivos CSV).

    Args:
        caminho_db (Path): Caminho para o banco de dados.
        caminho_gold (Path): Caminho para a pasta da camada Gold.

    Returns:
        bool: True se todas as validações passarem, False caso contrário.
    """
    logging.info("=" * 60)
    logging.info("🔍 VALIDANDO A CAMADA GOLD (VIEW E CSV)")
    logging.info("=" * 60)

    todas_ok = True
    conexao = None

    try:
        # --- Validação 1: View no Banco de Dados ---
        view_name = "vw_desmatamento_por_ano_estado"
        logging.info(f"1. Validando a VIEW '{view_name}' no banco de dados...")

        conexao = conectar_banco(caminho_db)
        cursor = conexao.cursor()

        # Checa se a view existe
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='view' AND name='{view_name}'")
        if cursor.fetchone() is None:
            logging.error(f"   ❌ A view '{view_name}' não foi encontrada no banco de dados!")
            todas_ok = False
        else:
            logging.info(f"   ✅ View '{view_name}' encontrada.")

            # Checa se a view tem registros
            cursor.execute(f"SELECT COUNT(*) FROM {view_name}")
            count = cursor.fetchone()[0]
            if count > 0:
                logging.info(f"   ✅ A view contém {count} registros.")
            else:
                logging.error(f"   ❌ A view '{view_name}' está vazia!")
                todas_ok = False

        # --- Validação 2: Arquivo CSV ---
        logging.info("")
        csv_filename = "desmatamento_por_ano_estado.csv"
        caminho_arquivo_gold = caminho_gold / csv_filename
        logging.info(f"2. Validando o arquivo CSV '{csv_filename}'...")

        if not caminho_arquivo_gold.exists():
            logging.error(f"   ❌ O arquivo CSV não foi encontrado em: {caminho_arquivo_gold}")
            todas_ok = False
        else:
            logging.info(f"   ✅ Arquivo CSV encontrado.")
            # Checa se o CSV tem dados
            try:
                df = pd.read_csv(caminho_arquivo_gold, sep=';')
                if not df.empty:
                    logging.info(f"   ✅ O arquivo CSV contém {len(df)} registros.")
                else:
                    logging.error("   ❌ O arquivo CSV está vazio!")
                    todas_ok = False
            except pd.errors.EmptyDataError:
                logging.error("   ❌ O arquivo CSV está vazio!")
                todas_ok = False

    except Exception as e:
        logging.error(f"   ❌ Erro inesperado durante a validação da camada Gold: {e}")
        todas_ok = False
    finally:
        if conexao:
            conexao.close()

    logging.info("-" * 60)
    if todas_ok:
        logging.info("✅ Validação da Camada Gold concluída com sucesso!")
    else:
        logging.error("❌ Validação da Camada Gold encontrou problemas.")
    logging.info("=" * 60)

    return todas_ok


if __name__ == "__main__":
    configurar_logs(caminho_log=PROJECT_ROOT / 'logs' / 'validate_gold.log')
    sucesso = validar_camada_gold()
    sys.exit(0 if sucesso else 1)