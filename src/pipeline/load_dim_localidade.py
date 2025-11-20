# Script para carregar a dimensão DimLocalidade
# Lê estados únicos da Silver e insere no banco (incremental) na tabela DimLocalidade

import logging
import pandas as pd
from utils import conectar_banco, ler_camada_silver, criar_tabelas, obter_regiao_por_estado, contar_registros_tabela


def carregar_dim_localidade(caminho_csv='data/silver/deforestation_silver_layer.csv',
                            caminho_db='db/desmatamento.db'):
    """
    Carrega a dimensão de localidade no Data Warehouse
    Insere apenas estados que ainda não existem no banco (incremental)

    Args:
        caminho_csv: Caminho para o arquivo Silver
        caminho_db: Caminho para o banco de dados

    Returns:
        Número de registros inseridos
    """

    logging.info("=" * 60)
    logging.info("📍 INICIANDO CARGA DA DIMENSÃO LOCALIDADE")
    logging.info("=" * 60)

    # Conecta ao banco
    conexao = conectar_banco(caminho_db)

    # Garante que as tabelas existem
    criar_tabelas(conexao)

    # Lê os dados do Silver
    df_silver = ler_camada_silver(caminho_csv)

    # Extrai estados únicos do arquivo
    estados_unicos = df_silver['estado'].unique()

    logging.info(f"🗺️ Encontrados {len(estados_unicos)} estados únicos no arquivo Silver")
    logging.info(f"   Estados: {', '.join(sorted(estados_unicos))}")

    # Busca estados que já existem no banco
    cursor = conexao.cursor()
    cursor.execute("SELECT estado FROM DimLocalidade")
    estados_existentes = set([row[0] for row in cursor.fetchall()])

    logging.info(f"📊 Estados já existentes no banco: {len(estados_existentes)}")

    # Filtra apenas estados novos (que não estão no banco)
    estados_novos = [estado for estado in estados_unicos if estado not in estados_existentes]

    if len(estados_novos) == 0:
        logging.info("✅ Nenhum estado novo para inserir (todos já estão no banco)")
        conexao.close()
        return 0

    logging.info(f"🆕 Estados novos para inserir: {len(estados_novos)}")
    logging.info(f"   Novos: {', '.join(sorted(estados_novos))}")

    # Insere os novos estados no banco
    registros_inseridos = 0

    for estado in estados_novos:
        try:
            # Determina a região do estado
            regiao = obter_regiao_por_estado(estado)

            cursor.execute("""
                INSERT INTO DimLocalidade (estado, regiao)
                VALUES (?, ?)
            """, (estado, regiao))

            registros_inseridos += 1
            logging.info(f"   ✓ {estado} ({regiao}) inserido")

        except Exception as e:
            logging.warning(f"⚠️ Erro ao inserir estado {estado}: {str(e)}")

    # Salva as mudanças
    conexao.commit()

    # Conta total de registros na tabela
    total_registros = contar_registros_tabela(conexao, 'DimLocalidade')

    logging.info(f"✅ {registros_inseridos} novos estados inseridos com sucesso!")
    logging.info(f"📊 Total de registros na DimLocalidade: {total_registros}")

    # Fecha conexão
    conexao.close()

    return registros_inseridos


if __name__ == "__main__":
    # Configura logs
    from utils import configurar_logs

    configurar_logs()

    # Executa a carga
    carregar_dim_localidade()