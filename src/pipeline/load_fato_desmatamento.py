#Script para carregar a tabela FatoDesmatamento
#Mapeia FKs das dimensões e insere os dados transformados

import logging
import pandas as pd
from utils import conectar_banco, ler_camada_silver, criar_tabelas, contar_registros_tabela


def buscar_id_tempo(conexao, data_completa):
    """
    Busca o ID de uma data na DimTempo

    Args:
        conexao: Conexão com o banco
        data_completa: Data no formato YYYY-MM-DD

    Returns:
        ID da data ou None se não encontrar
    """
    cursor = conexao.cursor()
    cursor.execute("SELECT id_tempo FROM DimTempo WHERE data_completa = ?", (data_completa,))
    resultado = cursor.fetchone()

    return resultado[0] if resultado else None


def buscar_id_localidade(conexao, estado):
    """
    Busca o ID de um estado na DimLocalidade

    Args:
        conexao: Conexão com o banco
        estado: Sigla do estado (ex: AM, PA)

    Returns:
        ID do estado ou None se não encontrar
    """
    cursor = conexao.cursor()
    cursor.execute("SELECT id_localidade FROM DimLocalidade WHERE estado = ?", (estado,))
    resultado = cursor.fetchone()

    return resultado[0] if resultado else None


def carregar_fato_desmatamento(caminho_csv='data/silver/deforestation_silver_layer.csv',
                               caminho_db='db/desmatamento.db'):
    """
    Carrega a tabela fato de desmatamento no Data Warehouse
    Mapeia as chaves estrangeiras das dimensões e insere os dados

    Args:
        caminho_csv: Caminho para o arquivo Silver
        caminho_db: Caminho para o banco de dados

    Returns:
        Número de registros inseridos
    """

    logging.info("=" * 60)
    logging.info("📊 INICIANDO CARGA DA TABELA FATO DESMATAMENTO")
    logging.info("=" * 60)

    # Conecta ao banco
    conexao = conectar_banco(caminho_db)

    # Garante que as tabelas existem
    criar_tabelas(conexao)

    # Lê os dados do Silver
    df_silver = ler_camada_silver(caminho_csv)

    logging.info(f"📄 Total de registros no arquivo Silver: {len(df_silver)}")

    # Limpa a tabela fato antes de inserir (pode ajustar conforme necessidade)
    # Comentado para permitir execução incremental
    # cursor = conexao.cursor()
    # cursor.execute("DELETE FROM FatoDesmatamento")
    # conexao.commit()
    # logging.info("🗑️ Tabela FatoDesmatamento limpa")

    # Cria um cache de IDs para melhorar performance
    cache_tempo = {}
    cache_localidade = {}

    logging.info("🔍 Construindo cache de dimensões...")

    # Cache DimTempo
    cursor = conexao.cursor()
    cursor.execute("SELECT id_tempo, data_completa FROM DimTempo")
    for id_tempo, data in cursor.fetchall():
        cache_tempo[data] = id_tempo

    # Cache DimLocalidade
    cursor.execute("SELECT id_localidade, estado FROM DimLocalidade")
    for id_localidade, estado in cursor.fetchall():
        cache_localidade[estado] = id_localidade

    logging.info(f"   ✓ Cache criado: {len(cache_tempo)} datas, {len(cache_localidade)} estados")

    # Insere os dados na tabela fato
    registros_inseridos = 0
    registros_com_erro = 0

    logging.info("💾 Iniciando inserção dos registros...")

    for indice, linha in df_silver.iterrows():
        try:
            # Busca os IDs das dimensões no cache
            id_tempo = cache_tempo.get(linha['data_imagem'])
            id_localidade = cache_localidade.get(linha['estado'])

            # Valida se encontrou os IDs
            if id_tempo is None:
                logging.warning(f"⚠️ Data não encontrada na DimTempo: {linha['data_imagem']}")
                registros_com_erro += 1
                continue

            if id_localidade is None:
                logging.warning(f"⚠️ Estado não encontrado na DimLocalidade: {linha['estado']}")
                registros_com_erro += 1
                continue

            # Insere na tabela fato
            cursor.execute("""
                INSERT INTO FatoDesmatamento (id_tempo, id_localidade, tipo_degradacao, area_km)
                VALUES (?, ?, ?, ?)
            """, (
                id_tempo,
                id_localidade,
                linha['tipo_degradacao'],
                float(linha['area_km'])
            ))

            registros_inseridos += 1

            # Mostra progresso a cada 1000 registros
            if registros_inseridos % 1000 == 0:
                logging.info(f"   ⏳ Processados: {registros_inseridos} registros...")

        except Exception as e:
            logging.warning(f"⚠️ Erro ao inserir registro {indice}: {str(e)}")
            registros_com_erro += 1

    # Salva as mudanças
    conexao.commit()

    # Estatísticas finais
    total_registros = contar_registros_tabela(conexao, 'FatoDesmatamento')

    logging.info("=" * 60)
    logging.info(f"✅ Carga concluída!")
    logging.info(f"   • Registros inseridos: {registros_inseridos}")
    logging.info(f"   • Registros com erro: {registros_com_erro}")
    logging.info(f"   • Total na tabela: {total_registros}")
    logging.info("=" * 60)

    # Mostra estatísticas dos dados inseridos
    cursor.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(area_km) as area_total,
            AVG(area_km) as area_media,
            MIN(area_km) as area_minima,
            MAX(area_km) as area_maxima
        FROM FatoDesmatamento
    """)

    stats = cursor.fetchone()
    logging.info("📈 ESTATÍSTICAS DOS DADOS:")
    logging.info(f"   • Total de registros: {stats[0]}")
    logging.info(f"   • Área total desmatada: {stats[1]:.2f} km²")
    logging.info(f"   • Área média por registro: {stats[2]:.6f} km²")
    logging.info(f"   • Menor área: {stats[3]:.6f} km²")
    logging.info(f"   • Maior área: {stats[4]:.2f} km²")

    # Fecha conexão
    conexao.close()

    return registros_inseridos


if __name__ == "__main__":
    # Configura logs
    from utils import configurar_logs

    configurar_logs()

    # Executa a carga
    carregar_fato_desmatamento()