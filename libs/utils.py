# libs/utils.py
import os
import logging
import warnings
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional
import psycopg2
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# ----------------------------
# 1) Silenciar warnings/logs
# ----------------------------
def silenciar_warnings() -> None:
    """Reduz verborragia de libs comuns em Spark."""
    warnings.filterwarnings("ignore")
    for name in ["py4j", "py4j.java_gateway", "botocore", "boto3", "urllib3"]:
        logging.getLogger(name).setLevel(logging.ERROR)
    logging.getLogger(__name__).info("🔕 Warnings e logs reduzidos.")

# ----------------------------
# 2) Carregar .env + config PG
# ----------------------------
@dataclass
class PgConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    jdbc_url: str
    jdbc_propriedades: dict

def pgconfig_init(dotenv_path: Optional[str] = None,
                   batchsize: int = 5000,
                   extra_jdbc_opts: Optional[dict] = None) -> PgConfig:
    """
    Carrega .env e monta JDBC para Spark.
    Retorna também credenciais para psycopg2.
    """
    # carregar variáveis do .env
    load_dotenv()

    host = os.getenv("POSTGRES_HOST")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    database = os.getenv("POSTGRES_DATABASE")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}?reWriteBatchedInserts=true"

    jdbc_propriedades = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
        "batchsize": "5000",
    }

    print("✅ Configuração do Postgres carregada.")

    return PgConfig(
        host=host, 
        port=port,
        database=database,
        user=user,
        password=password,
        jdbc_url=jdbc_url,
        jdbc_propriedades=jdbc_propriedades
    )

# ---------------------------------------
# 3) Conexão psycopg2
# ---------------------------------------
@contextmanager
def pg_conn(pgCfg: PgConfig):
    """
    Context manager que abre e fecha a conexão automaticamente.
    """
    conn = psycopg2.connect(
        host=pgCfg.host, port=pgCfg.port, database=pgCfg.database,
        user=pgCfg.user, password=pgCfg.password
    )
    try:
        yield conn              
        conn.commit()           # se tudo ok, comita o lote
    except Exception:
        conn.rollback()         # se qualquer comando falhar, reverte tudo
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass

def pg_executar_sql(conn, sql: str):
    """Executa SQL do parametro"""
    
    print("== Executando SQL no Postgres ==")
    print(sql)
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)

            # Se houver resultado (SELECT/RETURNING), cursor.description não é None
            if cursor.description is not None:
                rows = cursor.fetchall()
                print(f"✅ Query executada ({len(rows)} linhas).")
                conn.commit()  
                return rows

        conn.commit()
        print("✅ SQL executado!")

    except psycopg2.Error as e:
        conn.rollback()
        print(f"❌ Erro do Postgres: {e}")
        raise
    except Exception as e:
        conn.rollback()
        print(f"❌ Erro genérico: {e}")
        raise
    finally:
        print("== Processo finalizado ==")
        print("")

# ---------------------------------------
# 4) Funções para Spark
# ---------------------------------------

def spark_init(app_name: str = "TechChallenge3", master: str = "local[*]"):
    """Inicializa SparkSession com configs básicas."""

    try:
        if 'spark' in locals() and spark is not None:
            spark.stop()
            print("🔄 Spark session anterior fechada")
    except Exception as e:
        print(f"⚠️  Erro ao fechar sessão Spark: {e}")

    spark = (
        SparkSession.builder
            .appName(app_name)
            .master(master)
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "localhost")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
            .getOrCreate())
    
    spark.sparkContext.setLogLevel("ERROR")
    
    print(f"✅ Spark inicializado: {app_name} [{master}]")
    
    return spark

def spark_read_jdbc(spark: SparkSession = None, tabela: str = "", pgCfg: PgConfig = None, 
                    PARTES: int = 12, FETCHSIZE: str = "50000"):
    # Usar NTILE para criar uma coluna de partição sintética
    # Viabilizando leitura em paralelo 
    query = f"(SELECT *, ntile({PARTES}) OVER () AS __p FROM {tabela}) t"

    return (spark.read.format("jdbc")
            .option("url", pgCfg.jdbc_url)
            .option("dbtable", query)
            .option("partitionColumn", "__p")       # coluna numérica usada pra paralelizar a leitura
            .option("lowerBound", "1")              # menor valor esperado em __p
            .option("upperBound", str(PARTES))      # maior valor esperado em __p (= READ_PARTS)
            .option("numPartitions", str(PARTES))   # número de partições/tarefas em paralelo
            .option("fetchsize", FETCHSIZE)
            .options(**pgCfg.jdbc_propriedades)
            .load()
            .drop("__p"))                           # remove a coluna auxiliar; não é dado “de verdade”