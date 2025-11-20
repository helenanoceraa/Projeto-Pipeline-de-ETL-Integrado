# Script para criar a camada Gold
# Agrega os dados da camada Silver (Data Warehouse) e salva em um arquivo CSV.

import os
import logging
import pandas as pd
from utils import conectar_banco, configurar_logs

# --- Construção de Caminhos Absolutos ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..'))

DEFAULT_DB_PATH = os.path.join(PROJECT_ROOT, 'db', 'desmatamento.db')
GOLD_DATA_PATH = os.path.join(PROJECT_ROOT, 'data', 'gold')


def criar_camada_gold(caminho_db=DEFAULT_DB_PATH,
                      caminho_gold=GOLD_DATA_PATH):
    """
    Cria uma tabela agregada (camada Gold) a partir dos dados do Data Warehouse.

    A tabela agregada conterá:
    - ano
    - estado
    - regiao
    - total_area_desmatada_km

    Args:
        caminho_db (str): Caminho para o banco de dados do DW.
        caminho_gold (str): Caminho para a pasta onde o arquivo gold será salvo.
    """
    logging.info("=" * 60)
    logging.info("🥇 INICIANDO CRIAÇÃO DA CAMADA GOLD")
    logging.info("=" * 60)

    try:
        # Conecta ao banco de dados
        conexao = conectar_banco(caminho_db)
        cursor = conexao.cursor()
        logging.info(f"🔗 Conectado ao banco de dados: {caminho_db}")

        # Query SQL para agregar os dados
        query_gold = """
        SELECT
            t.ano,
            -- Transforma '2022-07-15' em '2022-07'
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
            -- Dica: COUNT(coluna) já ignora nulos automaticamente, é mais rápido que o SUM CASE
            COUNT(f.area_km) AS qtd_ocorrencias,
            ROUND(SUM(f.area_km), 2) as total_area_desmatada_km
        FROM FatoDesmatamento f
        JOIN DimTempo t ON f.id_tempo = t.id_tempo
        JOIN DimLocalidade l ON f.id_localidade = l.id_localidade
        GROUP BY 1, 2, 3, 4, 5
        ORDER BY 1 DESC, 3;
        """
        logging.info("📄 Executando query de agregação no banco de dados...")

        # Executa a query e carrega o resultado em um DataFrame Pandas
        df_gold = pd.read_sql_query(query_gold, conexao)
        logging.info(f"📊 {len(df_gold)} registros agregados gerados.")

        # --- Criação da VIEW no banco de dados ---
        view_name = "vw_desmatamento_por_ano_estado"
        logging.info(f"🏗️  Criando/Recriando a VIEW: {view_name}")

        # Apaga a view se ela já existir (para garantir que está atualizada)
        cursor.execute(f"DROP VIEW IF EXISTS {view_name}")

        # Cria a nova view usando a mesma query
        create_view_query = f"CREATE VIEW {view_name} AS {query_gold}"
        cursor.execute(create_view_query)
        conexao.commit()
        logging.info(f"   ✅ VIEW '{view_name}' criada com sucesso no banco de dados.")
        # --- Fim da criação da VIEW ---

        # Garante que o diretório gold exista
        os.makedirs(caminho_gold, exist_ok=True)

        # Salva o resultado em um arquivo CSV
        caminho_arquivo_gold = os.path.join(caminho_gold, 'desmatamento_por_ano_estado.csv')
        df_gold.to_csv(caminho_arquivo_gold, index=False, sep=';', decimal=',')
        logging.info(f"✅ Camada Gold salva com sucesso em: {caminho_arquivo_gold}")

        return True
    except Exception as e:
        logging.error(f"❌ Erro ao criar a camada Gold: {e}")
    finally:
        if 'conexao' in locals() and conexao:
            conexao.close()
            logging.info("🔌 Conexão com o banco de dados fechada.")

if __name__ == "__main__":
    configurar_logs(caminho_log='logs/create_gold_layer.log')
    criar_camada_gold()
