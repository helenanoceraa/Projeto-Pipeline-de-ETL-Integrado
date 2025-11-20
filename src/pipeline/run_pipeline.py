# Script principal que executa toda a pipeline de carga e roda TUDO em ordem:
# 1. Carrega DimTempo
# 2. Carrega DimLocalidade
# 3. Carrega FatoDesmatamento
# 4. Mostra logs de quantos registros foram inseridos
# 5. Faz checagens básicas (tem dados? tem erros?)

import sys
from datetime import datetime
from pathlib import Path

# Importa as funções de carga
from utils import configurar_logs, conectar_banco, contar_registros_tabela
from load_dim_tempo import carregar_dim_tempo
from load_dim_localidade import carregar_dim_localidade
from load_fato_desmatamento import carregar_fato_desmatamento


# Define o caminho raiz do projeto (a pasta que contém 'src', 'data', etc.)
PROJECT_ROOT = Path(__file__).parent.parent.parent


def validar_arquivos(caminho_csv):
    """
    Valida se todos os arquivos necessários existem antes de começar

    Args:
        caminho_csv: Caminho para o arquivo Silver

    Returns:
        True se tudo OK, False se houver problema
    """
    import logging

    caminho_silver = Path(caminho_csv)

    if not caminho_silver.exists():
        logging.error(f"❌ ERRO: Arquivo Silver não encontrado!")
        logging.error(f"   Caminho esperado: {caminho_silver.absolute()}")
        return False

    logging.info(f"✅ Arquivo Silver encontrado: {caminho_silver}")

    return True


def validar_integridade_dados(caminho_db):
    """
    Faz checagens básicas de integridade dos dados carregados

    Args:
        caminho_db: Caminho para o banco de dados
    """
    import logging

    logging.info("=" * 60)
    logging.info("🔍 VALIDANDO INTEGRIDADE DOS DADOS")
    logging.info("=" * 60)

    conexao = conectar_banco(caminho_db)
    cursor = conexao.cursor()

    # Checa se há registros em todas as tabelas
    tabelas = ['DimTempo', 'DimLocalidade', 'FatoDesmatamento']
    todas_ok = True

    for tabela in tabelas:
        count = contar_registros_tabela(conexao, tabela)

        if count > 0:
            logging.info(f"   ✅ {tabela}: {count} registros")
        else:
            logging.error(f"   ❌ {tabela}: VAZIA!")
            todas_ok = False

    # Checa integridade referencial (FKs)
    logging.info("")
    logging.info("🔗 Checando integridade referencial...")

    # Checa FKs de tempo
    cursor.execute("""
        SELECT COUNT(*) 
        FROM FatoDesmatamento f
        LEFT JOIN DimTempo d ON f.id_tempo = d.id_tempo
        WHERE d.id_tempo IS NULL
    """)
    fks_tempo_quebradas = cursor.fetchone()[0]

    if fks_tempo_quebradas == 0:
        logging.info(f"   ✅ Todas as FKs de tempo estão corretas")
    else:
        logging.error(f"   ❌ {fks_tempo_quebradas} FKs de tempo quebradas!")
        todas_ok = False

    # Checa FKs de localidade
    cursor.execute("""
        SELECT COUNT(*) 
        FROM FatoDesmatamento f
        LEFT JOIN DimLocalidade d ON f.id_localidade = d.id_localidade
        WHERE d.id_localidade IS NULL
    """)
    fks_local_quebradas = cursor.fetchone()[0]

    if fks_local_quebradas == 0:
        logging.info(f"   ✅ Todas as FKs de localidade estão corretas")
    else:
        logging.error(f"   ❌ {fks_local_quebradas} FKs de localidade quebradas!")
        todas_ok = False

    # Checa se há valores nulos na fato
    cursor.execute("""
        SELECT COUNT(*) 
        FROM FatoDesmatamento
        WHERE area_km IS NULL OR area_km = 0
    """)
    areas_invalidas = cursor.fetchone()[0]

    if areas_invalidas == 0:
        logging.info(f"   ✅ Todas as áreas têm valores válidos")
    else:
        logging.warning(f"   ⚠️ {areas_invalidas} registros com área nula ou zero")

    conexao.close()

    if todas_ok:
        logging.info("")
        logging.info("✅ INTEGRIDADE DOS DADOS: OK")
    else:
        logging.error("")
        logging.error("❌ PROBLEMAS DE INTEGRIDADE ENCONTRADOS!")

    logging.info("=" * 60)

    return todas_ok


def executar_pipeline(caminho_csv, caminho_db):
    """
    Executa toda a pipeline de carga do Data Warehouse

    Args:
        caminho_csv: Caminho para o arquivo Silver
        caminho_db: Caminho para o banco de dados

    Returns:
        True se sucesso, False se houver erro
    """
    import logging

    # Marca hora de início
    hora_inicio = datetime.now()

    logging.info("")
    logging.info("╔" + "=" * 58 + "╗")
    logging.info("║" + " " * 10 + "PIPELINE DE CARGA - DATA WAREHOUSE" + " " * 13 + "║")
    logging.info("║" + " " * 15 + "Análise de Desmatamento" + " " * 20 + "║")
    logging.info("╚" + "=" * 58 + "╝")
    logging.info("")
    logging.info(f"🕐 Início: {hora_inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info("")

    try:
        # ETAPA 1: Validação dos arquivos
        logging.info("📋 ETAPA 1/4: Validando arquivos necessários...")
        if not validar_arquivos(caminho_csv):
            return False
        logging.info("")

        # ETAPA 2: Carga das dimensões
        logging.info("📋 ETAPA 2/4: Carregando dimensões...")
        logging.info("")

        # Carrega DimTempo
        registros_tempo = carregar_dim_tempo(caminho_csv, caminho_db)
        logging.info("")

        # Carrega DimLocalidade
        registros_localidade = carregar_dim_localidade(caminho_csv, caminho_db)
        logging.info("")

        # ETAPA 3: Carga da tabela fato
        logging.info("📋 ETAPA 3/4: Carregando tabela fato...")
        logging.info("")

        registros_fato = carregar_fato_desmatamento(caminho_csv, caminho_db)
        logging.info("")

        # ETAPA 4: Validação da integridade
        logging.info("📋 ETAPA 4/4: Validando integridade dos dados...")
        logging.info("")

        integridade_ok = validar_integridade_dados(caminho_db)
        logging.info("")

        # Calcula tempo de execução
        hora_fim = datetime.now()
        tempo_execucao = hora_fim - hora_inicio

        # Resumo final
        logging.info("╔" + "=" * 58 + "╗")
        logging.info("║" + " " * 18 + "RESUMO DA EXECUÇÃO" + " " * 21 + "║")
        logging.info("╚" + "=" * 58 + "╝")
        logging.info("")
        logging.info(f"✅ Pipeline executada com sucesso!")
        logging.info("")
        logging.info("📊 Registros processados:")
        logging.info(f"   • DimTempo: {registros_tempo} novos registros")
        logging.info(f"   • DimLocalidade: {registros_localidade} novos registros")
        logging.info(f"   • FatoDesmatamento: {registros_fato} novos registros")
        logging.info("")
        logging.info(f"🕐 Tempo de execução: {tempo_execucao.total_seconds():.2f} segundos")
        logging.info(f"📁 Banco de dados: {Path(caminho_db).absolute()}")
        logging.info(f"📝 Log salvo em: logs/pipeline_run.log")
        logging.info("")

        if integridade_ok:
            logging.info("✅ Integridade dos dados: OK")
        else:
            logging.warning("⚠️ Atenção: Alguns problemas de integridade foram detectados")

        logging.info("")
        logging.info("=" * 60)

        return True

    except Exception as e:
        logging.error("")
        logging.error("=" * 60)
        logging.error("❌ ERRO DURANTE A EXECUÇÃO DA PIPELINE")
        logging.error(f"   {str(e)}")
        logging.error("=" * 60)
        logging.error("")

        import traceback
        logging.error("Detalhes do erro:")
        logging.error(traceback.format_exc())

        return False


if __name__ == "__main__":
    # Configura o sistema de logs
    logger = configurar_logs()

    # Executa a pipeline
    caminho_csv_silver = PROJECT_ROOT / 'data' / 'silver' / 'deforestation_silver_layer.csv'
    caminho_banco_dados = PROJECT_ROOT / 'db' / 'desmatamento.db'

    sucesso = executar_pipeline(caminho_csv=caminho_csv_silver,
                                caminho_db=caminho_banco_dados)

    # Retorna código de saída apropriado
    sys.exit(0 if sucesso else 1)