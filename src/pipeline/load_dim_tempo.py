# Script para carregar a dimensão DimTempo
# Lê datas únicas do Silver e insere no banco (incremental)

import logging
import pandas as pd
from utils import conectar_banco, ler_camada_silver, criar_tabelas, contar_registros_tabela


def carregar_dim_tempo(caminho_csv='data/silver/deforestation_silver_layer.csv',
                       caminho_db='db/desmatamento.db'):
    """
    Carrega a dimensão de tempo no Data Warehouse
    Insere apenas datas que ainda não existem no banco (incremental)

    Args:
        caminho_csv: Caminho para o arquivo Silver
        caminho_db: Caminho para o banco de dados

    Returns:
        Número de registros inseridos
    """

    logging.info("=" * 60)
    logging.info("🕐 INICIANDO CARGA DA DIMENSÃO TEMPO")
    logging.info("=" * 60)

    # Conecta ao banco
    conexao = conectar_banco(caminho_db)

    # Garante que as tabelas existem
    criar_tabelas(conexao)

    # Lê os dados do Silver
    df_silver = ler_camada_silver(caminho_csv)

    # Extrai datas únicas do arquivo
    df_tempo = df_silver[['data_imagem', 'ano', 'mes', 'dia', 'ano_mes', 'semestre']].copy()
    df_tempo = df_tempo.drop_duplicates(subset=['data_imagem'])
    df_tempo = df_tempo.rename(columns={'data_imagem': 'data_completa'})

    logging.info(f"📅 Encontradas {len(df_tempo)} datas únicas no arquivo Silver")

    # Busca datas que já existem no banco
    cursor = conexao.cursor()
    cursor.execute("SELECT data_completa FROM DimTempo")
    datas_existentes = set([row[0] for row in cursor.fetchall()])

    logging.info(f"📊 Datas já existentes no banco: {len(datas_existentes)}")

    # Filtra apenas datas novas (que não estão no banco)
    df_tempo_novo = df_tempo[~df_tempo['data_completa'].isin(datas_existentes)]

    if len(df_tempo_novo) == 0:
        logging.info("✅ Nenhuma data nova para inserir (todas já estão no banco)")
        conexao.close()
        return 0

    logging.info(f"🆕 Datas novas para inserir: {len(df_tempo_novo)}")

    # Insere as novas datas no banco
    registros_inseridos = 0

    for _, linha in df_tempo_novo.iterrows():
        try:
            cursor.execute("""
                INSERT INTO DimTempo (data_completa, ano, mes, dia, ano_mes, semestre)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                linha['data_completa'],
                int(linha['ano']),
                int(linha['mes']),
                int(linha['dia']),
                linha['ano_mes'],
                int(linha['semestre'])
            ))
            registros_inseridos += 1

        except Exception as e:
            logging.warning(f"⚠️ Erro ao inserir data {linha['data_completa']}: {str(e)}")

    # Salva as mudanças
    conexao.commit()

    # Conta total de registros na tabela
    total_registros = contar_registros_tabela(conexao, 'DimTempo')

    logging.info(f"✅ {registros_inseridos} novas datas inseridas com sucesso!")
    logging.info(f"📊 Total de registros na DimTempo: {total_registros}")

    # Fecha conexão
    conexao.close()

    return registros_inseridos


if __name__ == "__main__":
    # Configura logs
    from utils import configurar_logs

    configurar_logs()

    # Executa a carga
    carregar_dim_tempo()