# Script para criar as VIEWS da camada Gold no Data Warehouse.

import os
import logging
from pathlib import Path

# Importação absoluta a partir da raiz do pacote 'pipeline'
from utils import conectar_banco, configurar_logs

# --- Construção de Caminhos Absolutos ---
# Usando pathlib para uma manipulação de caminhos mais moderna e segura.
# O caminho vai subir 2 níveis a partir do arquivo atual: pipeline -> src -> PROJECT_ROOT
PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_DB_PATH = PROJECT_ROOT / 'db' / 'desmatamento.db'


def criar_views_gold(caminho_db=DEFAULT_DB_PATH):
    """
    Cria ou recria as views agregadas (camada Gold) no Data Warehouse.

    Views criadas:
    - vw_desmatamento_agregado: Agrega dados de desmatamento por ano, mês, estado e tipo.

    Args:
        caminho_db (str): Caminho para o banco de dados do DW.

    Returns:
        bool: True se a operação for bem-sucedida, False caso contrário.
    """
    logging.info("=" * 60)
    logging.info("🏗️  INICIANDO CRIAÇÃO/ATUALIZAÇÃO DE VIEWS (GOLD)")
    logging.info("=" * 60)

    try:
        conexao = conectar_banco(caminho_db)
        cursor = conexao.cursor()
        logging.info(f"🔗 Conectado ao banco de dados: {caminho_db}")

        # Query SQL para a view de desmatamento agregado
        query_view = """
        SELECT
            t.ano,
            strftime('%Y-%m', t.data_completa) as safra_ocorrido,
            l.estado,
            l.regiao,
            CASE 
                WHEN LOWER(f.tipo_degradacao) LIKE '%minera%' OR LOWER(f.tipo_degradacao) LIKE '%garimpo%' THEN 'Mineração'
                WHEN LOWER(f.tipo_degradacao) LIKE '%solo%' THEN 'Corte Raso com Solo Exposto'
                WHEN LOWER(f.tipo_degradacao) LIKE '%vegeta%' THEN 'Corte Raso com Vegetação'
                WHEN LOWER(f.tipo_degradacao) LIKE '%inunda%' THEN 'Floresta Inundada'
                WHEN LOWER(f.tipo_degradacao) LIKE '%degrada%' THEN 'Desmatamento por Degradação Progressiva'
                ELSE 'Outros'
            END AS tipo_desmatamento,
            -- COUNT(coluna) já ignora nulos e é mais eficiente que SUM(CASE...)
            COUNT(f.area_km) AS qtd_ocorrencias,
            ROUND(SUM(f.area_km), 2) as total_area_desmatada_km
        FROM FatoDesmatamento f
        JOIN DimTempo t ON f.id_tempo = t.id_tempo
        JOIN DimLocalidade l ON f.id_localidade = l.id_localidade
        GROUP BY t.ano, safra_ocorrido, l.estado, l.regiao, tipo_desmatamento
        """

        view_name = "vw_desmatamento_agregado"
        logging.info(f"   -> Criando/Recriando a VIEW: {view_name}")
        cursor.execute(f"DROP VIEW IF EXISTS {view_name}")

        # Constrói o comando CREATE VIEW de forma segura
        create_view_sql = f"CREATE VIEW {view_name} AS {query_view}"
        cursor.execute(create_view_sql)

        logging.info(f"   ✅ VIEW '{view_name}' criada com sucesso.")
        return True

    except Exception as e:
        logging.error(f"❌ Erro ao criar as views da camada Gold: {e}")
        return False
    finally:
        if 'conexao' in locals() and conexao:
            conexao.close()
            logging.info("🔌 Conexão com o banco de dados fechada.")

if __name__ == "__main__":
    configurar_logs(caminho_log=PROJECT_ROOT / 'logs' / 'create_views.log')
    criar_views_gold()
