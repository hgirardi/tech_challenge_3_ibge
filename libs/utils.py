# libs/utils.py
import os
import logging
import warnings
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional

import psycopg2
from dotenv import load_dotenv

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