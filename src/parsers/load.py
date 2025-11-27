"""
Módulo load.py — Fase 3 da Pipeline (LOAD)

Responsável por:
- Ler os CSVs normalizados em output/norm_csv/
- Conectar ao banco PostgreSQL (poc_xml)
- Carregar os dados nas tabelas relacionais
- Garantir idempotência (ON CONFLICT DO NOTHING)

Arquitetura:
- Usa SQLAlchemy para conexão e reflexão das tabelas
- Usa pandas para leitura dos CSVs
- Usa logging_utils para logs padronizados

Autor: Angelo & Assistente
Versão: 1.0
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import List, Dict

import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert as pg_insert

from common.logging_utils import log_info, log_ok, log_error
from sqlalchemy import text



# ---------------------------------------------------------------------------
# CONSTANTES DE PROJETO
# ---------------------------------------------------------------------------

# Diretório base dentro do container (WORKDIR /app)
BASE_DIR = Path(".").resolve()

# Diretório dos CSVs normalizados (saída da Fase 2 – NORMALIZE)
NORM_DIR = BASE_DIR / "output" / "norm_csv"


# Ordem de carga respeitando dependências de chaves estrangeiras
TABLE_LOAD_ORDER = [
    ("relays_core", "relays_core.csv"),
    ("relays_cts", "relays_cts.csv"),
    ("relays_vts", "relays_vts.csv"),
    ("relays_functions", "relays_functions.csv"),
    ("relays_function_settings", "relays_function_settings.csv"),
    ("relays_curves", "relays_curves.csv"),
    ("relays_curve_points", "relays_curve_points.csv"),
    ("relays_parameters", "relays_parameters.csv"),
    ("relays_selectivity", "relays_selectivity.csv"),
]


# ---------------------------------------------------------------------------
# FUNÇÕES DE SUPORTE
# ---------------------------------------------------------------------------


def get_database_url() -> str:
    """Obtém a URL de conexão com o banco PostgreSQL.

    A função tenta primeiro usar a variável de ambiente DATABASE_URL.
    Caso não esteja definida, utiliza um valor padrão adequado para
    o ambiente Docker da POC (serviço 'postgres' na rede do compose).

    Returns:
        str: URL de conexão no formato SQLAlchemy.
    """
    env_url = os.getenv("DATABASE_URL")
    if env_url:
        log_info(f"Usando DATABASE_URL do ambiente: {env_url}")
        return env_url

    # Fallback padrão para o ambiente docker-compose (rede interna)
    default_url = "postgresql+psycopg2://p_xml:p_xml@postgres:5432/poc_xml"
    log_info("DATABASE_URL não definida. Usando URL padrão interna Docker.")
    log_info(f"URL padrão: {default_url}")
    return default_url


def create_engine_from_env() -> Engine:
    """Cria o engine SQLAlchemy a partir da URL de banco.

    Returns:
        Engine: Instância de engine SQLAlchemy conectável ao PostgreSQL.
    """
    db_url = get_database_url()

    try:
        engine = create_engine(db_url, future=True)
        # Teste leve de conexão
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        log_ok("Conexão com o PostgreSQL estabelecida com sucesso.")
        return engine
    except SQLAlchemyError as exc:
        log_error(f"Falha ao conectar no banco de dados: {exc}")
        sys.exit(1)


def read_csv_safe(csv_path: Path) -> pd.DataFrame:
    """Lê um CSV de forma segura, com tratamento de erros.

    Args:
        csv_path (Path): Caminho completo para o arquivo CSV.

    Returns:
        pd.DataFrame: DataFrame com os dados do CSV.
    """
    if not csv_path.exists():
        log_error(f"CSV não encontrado: {csv_path}")
        return pd.DataFrame()

    try:
        df = pd.read_csv(csv_path)
        # Substitui NaN por None para compatibilidade com o PostgreSQL
        df = df.where(pd.notnull(df), None)
        return df
    except Exception as exc:  # noqa: BLE001
        log_error(f"Erro ao ler CSV '{csv_path}': {exc}")
        return pd.DataFrame()


def filter_columns_to_table(df: pd.DataFrame, table: Table) -> pd.DataFrame:
    """Filtra as colunas do DataFrame para manter apenas as colunas da tabela.

    Isso garante que:
    - Colunas extras no CSV sejam ignoradas com segurança.
    - Somente colunas válidas sejam usadas no INSERT.

    Args:
        df (pd.DataFrame): DataFrame lido do CSV.
        table (Table): Tabela refletida via SQLAlchemy.

    Returns:
        pd.DataFrame: DataFrame apenas com colunas compatíveis com a tabela.
    """
    if df.empty:
        return df

    table_cols = {col.name for col in table.columns}
    df_cols = set(df.columns)

    common = [c for c in df.columns if c in table_cols]

    if not common:
        log_error(
            "Nenhuma coluna em comum entre CSV e tabela. "
            f"Tabela: {table.name} | CSV cols: {sorted(df_cols)} | "
            f"Tabela cols: {sorted(table_cols)}"
        )
        return pd.DataFrame()

    # Loga colunas extras apenas como aviso
    extra = df_cols - table_cols
    if extra:
        log_info(
            f"Tabela '{table.name}': colunas extras no CSV serão ignoradas: "
            f"{sorted(extra)}"
        )

    return df[common]


def upsert_dataframe(
    engine: Engine,
    table: Table,
    df: pd.DataFrame,
    pk_columns: List[str],
) -> None:
    """Realiza INSERT com ON CONFLICT DO NOTHING para um DataFrame.

    Args:
        engine (Engine): Engine SQLAlchemy conectado ao PostgreSQL.
        table (Table): Tabela alvo refletida.
        df (pd.DataFrame): DataFrame com os dados a serem inseridos.
        pk_columns (List[str]): Lista de colunas que compõem a chave primária.
    """
    if df.empty:
        log_info(f"Nenhuma linha para inserir em '{table.name}'.")
        return

    records: List[Dict] = df.to_dict(orient="records")

    try:
        with engine.begin() as conn:
            stmt = pg_insert(table).values(records)

            if pk_columns:
                stmt = stmt.on_conflict_do_nothing(
                    index_elements=pk_columns,
                )

            conn.execute(stmt)

        log_ok(
            f"Carregadas {len(records)} linhas na tabela "
            f"'{table.name}' (ON CONFLICT DO NOTHING)."
        )
    except SQLAlchemyError as exc:
        log_error(f"Erro ao inserir dados na tabela '{table.name}': {exc}")


# ---------------------------------------------------------------------------
# PIPELINE DE CARGA
# ---------------------------------------------------------------------------


def load_table_from_csv(
    engine: Engine,
    metadata: MetaData,
    table_name: str,
    csv_filename: str,
    pk_columns: List[str],
) -> None:
    """Carrega um CSV específico para a tabela correspondente.

    Fluxo:
    - Reflete a tabela pelo metadata
    - Lê o CSV em output/norm_csv
    - Filtra colunas compatíveis
    - Realiza upsert com ON CONFLICT DO NOTHING

    Args:
        engine (Engine): Engine SQLAlchemy.
        metadata (MetaData): Metadata SQLAlchemy refletindo o schema.
        table_name (str): Nome da tabela no banco.
        csv_filename (str): Nome do arquivo CSV a carregar.
        pk_columns (List[str]): Colunas que formam a PK para conflito.
    """
    csv_path = NORM_DIR / csv_filename

    log_info(
        f"Carregando CSV '{csv_filename}' para a tabela '{table_name}'..."
    )

    # Garante que a tabela existe no metadata
    try:
        table = Table(table_name, metadata, autoload_with=engine)
    except SQLAlchemyError as exc:
        log_error(f"Erro ao refletir tabela '{table_name}': {exc}")
        return

    df = read_csv_safe(csv_path)
    if df.empty:
        log_info(
            f"CSV '{csv_filename}' está vazio ou não pôde ser lido. "
            f"Tabela '{table_name}' não será alterada."
        )
        return

    df = filter_columns_to_table(df, table)
    if df.empty:
        log_error(
            f"Após filtragem de colunas, não há dados válidos para "
            f"a tabela '{table_name}'."
        )
        return

    upsert_dataframe(engine, table, df, pk_columns)


def run_load_pipeline() -> None:
    """Orquestra a Fase 3 – LOAD (CSV → PostgreSQL).

    Etapas:
    1. Cria conexão com o banco
    2. Reflete o schema existente (tabelas criadas via init.sql)
    3. Percorre os CSVs normalizados em ordem lógica
    4. Executa INSERT com ON CONFLICT DO NOTHING para cada tabela
    """
    log_info("Iniciando Fase LOAD (CSV → PostgreSQL)...")

    if not NORM_DIR.exists():
        log_error(
            f"Diretório de CSVs normalizados não encontrado: {NORM_DIR}"
        )
        sys.exit(1)

    engine = create_engine_from_env()
    metadata = MetaData()

    # Reflete todas as tabelas existentes
    try:
        metadata.reflect(bind=engine)
        log_ok(
            f"Tabelas refletidas com sucesso: "
            f"{sorted(metadata.tables.keys())}"
        )
    except SQLAlchemyError as exc:
        log_error(f"Erro ao refletir metadata do banco: {exc}")
        sys.exit(1)

    # Mapeamento das PKs por tabela (IDs vindos do XML / normalize)
    pk_map = {
        "relays_core": ["relay_id"],
        "relays_cts": ["ct_id"],
        "relays_vts": ["vt_id"],
        "relays_functions": ["function_id"],
        "relays_function_settings": ["setting_id"],
        "relays_curves": ["curve_id"],
        "relays_curve_points": ["point_id"],
        "relays_parameters": ["parameter_id"],
        "relays_selectivity": ["selectivity_id"],
    }

    for table_name, csv_filename in TABLE_LOAD_ORDER:
        pk_columns = pk_map.get(table_name, [])
        load_table_from_csv(
            engine=engine,
            metadata=metadata,
            table_name=table_name,
            csv_filename=csv_filename,
            pk_columns=pk_columns,
        )

    log_ok("Fase LOAD finalizada com sucesso.")


# ---------------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run_load_pipeline()
