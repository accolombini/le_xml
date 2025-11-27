"""
Módulo extract.py — Fase 1 da Pipeline (EXTRACT)

Responsável por:
- Ler o XML bruto da subestação 13,8 kV
- Converter o XML para dicionário (via xmltodict)
- Remover namespaces dos elementos
- Localizar automaticamente:
    - Nó raiz <ProtectionDataset>
    - Nó <Substation> (topologia)
    - Nó <Relays> (proteção)
- Extrair entidades principais:
    - Barras (Buses)
    - Transformadores (Transformers)
    - Alimentadores (Feeders)
    - Cargas (Loads)
    - Disjuntores (Breakers)
    - Relés (Relays – visão de alto nível)
- Gerar arquivos CSV em output/csv/

Observação:
A normalização até 3FN (separação detalhada de CTs, VTs, Funções,
Curvas, Pontos etc.) será feita na Fase 2 (NORMALIZE). Aqui trabalhamos
com uma extração “bruta”, porém estruturada, diretamente do XML.

Autor: Angelo & Assistente
Versão: 2.0
"""

import os
import sys
from typing import Any, Dict, List, Optional

import pandas as pd
import xmltodict

from common.logging_utils import log_info, log_ok, log_error


# ----------------------------------------------------------------------
# UTILITÁRIOS DE XML
# ----------------------------------------------------------------------


def load_xml(path: str) -> Dict[str, Any]:
    """
    Carrega o arquivo XML, remove namespaces e retorna um dicionário limpo.

    Namespaces do tipo "ns0:" são removidos das chaves, mantendo apenas
    o nome local das tags (ProtectionDataset, Substation, Relays etc.).

    Args:
        path: Caminho para o arquivo XML.

    Returns:
        dict: Dicionário correspondente ao XML, com namespaces removidos.

    Raises:
        SystemExit: Em caso de erro na leitura ou parsing do XML.
    """
    log_info(f"Lendo arquivo XML: {path}")

    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = f.read()
            data = xmltodict.parse(raw)

        def strip_ns(obj: Any) -> Any:
            """
            Remove namespaces (prefixos 'ns0:' etc.) recursivamente.
            """
            if isinstance(obj, dict):
                new_dict: Dict[str, Any] = {}
                for k, v in obj.items():
                    clean_key = k.split(":")[-1]
                    new_dict[clean_key] = strip_ns(v)
                return new_dict
            if isinstance(obj, list):
                return [strip_ns(i) for i in obj]
            return obj

        cleaned = strip_ns(data)

        log_ok("XML carregado e normalizado (namespaces removidos).")
        return cleaned

    except Exception as exc:
        log_error(f"Erro ao ler/parsing do XML: {exc}")
        sys.exit(1)


def get_root_dataset(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Obtém o nó raiz <ProtectionDataset> do XML já normalizado.

    Args:
        data: Dicionário retornado por load_xml().

    Returns:
        dict: Nó correspondente a <ProtectionDataset>.

    Raises:
        SystemExit: Se o nó não for encontrado.
    """
    root = data.get("ProtectionDataset")
    if root is None:
        # Fallback defensivo: alguns parsers podem aninhar em outro nível
        # (caso geral, pegamos o primeiro valor se fizer sentido).
        possible_keys = list(data.keys())
        log_error(
            "Nó <ProtectionDataset> não encontrado no XML normalizado. "
            f"Chaves de topo encontradas: {possible_keys}"
        )
        sys.exit(1)
    return root


def get_substation(root: Dict[str, Any]) -> Dict[str, Any]:
    """
    Retorna o nó <Substation> a partir do nó raiz <ProtectionDataset>.

    Args:
        root: Dicionário correspondente a <ProtectionDataset>.

    Returns:
        dict: Nó <Substation>.

    Raises:
        SystemExit: Se o nó Substation não for encontrado.
    """
    sub = root.get("Substation")
    if sub is None:
        log_error("Nó <Substation> não encontrado dentro de <ProtectionDataset>.")
        sys.exit(1)
    return sub


def get_relays_root(root: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Retorna o nó <Relays> a partir do nó raiz <ProtectionDataset>.

    Os relés, neste schema, ficam fora da Substation, como bloco global
    de proteção.

    Args:
        root: Dicionário correspondente a <ProtectionDataset>.

    Returns:
        dict | None: Nó <Relays>, se existir; caso contrário, None.
    """
    relays_root = root.get("Relays")
    return relays_root


def ensure_list(obj: Any) -> List[Any]:
    """
    Garante que o objeto seja uma lista.

    - Se for None → retorna lista vazia.
    - Se já for lista → retornada como está.
    - Caso contrário → embrulha em uma lista [obj].

    Args:
        obj: Objeto de entrada (possivelmente dict, list ou None).

    Returns:
        list: Lista normalizada.
    """
    if obj is None:
        return []
    if isinstance(obj, list):
        return obj
    return [obj]


def save_csv(df: pd.DataFrame, name: str) -> None:
    """
    Salva um DataFrame em formato CSV dentro de output/csv/.

    Se o DataFrame estiver vazio, ainda assim gera o arquivo, mas com
    uma mensagem de aviso para facilitar depuração.

    Args:
        df: DataFrame a ser salvo.
        name: Nome base do arquivo (sem extensão).
    """
    os.makedirs("output/csv", exist_ok=True)
    out_path = os.path.join("output", "csv", f"{name}.csv")

    if df.empty:
        log_info(f"[AVISO] DataFrame para '{name}' está vazio. CSV será gerado sem linhas.")

    df.to_csv(out_path, index=False)
    log_ok(f"Arquivo gerado: {out_path}")


# ----------------------------------------------------------------------
# FUNÇÕES DE EXTRAÇÃO — TOPOLOGIA
# ----------------------------------------------------------------------


def extract_buses(sub: Dict[str, Any]) -> pd.DataFrame:
    """
    Extrai as barras (Buses) da Substation.

    Estrutura esperada (já sem namespaces):
        Substation
          └── Buses
                └── Bus (lista ou único)

    Args:
        sub: Nó <Substation>.

    Returns:
        pd.DataFrame: Tabela de barras.
    """
    buses_block = sub.get("Buses")
    if buses_block is None:
        log_info("[AVISO] Bloco <Buses> não encontrado em <Substation>.")
        return pd.DataFrame()

    buses = ensure_list(buses_block.get("Bus"))
    if not buses:
        log_info("[AVISO] Nenhum <Bus> encontrado dentro de <Buses>.")
        return pd.DataFrame()

    return pd.DataFrame(buses)


def extract_transformers(sub: Dict[str, Any]) -> pd.DataFrame:
    """
    Extrai os transformadores da Substation.

    Estrutura esperada:
        Substation
          └── Transformers
                └── Transformer (lista ou único)

    Args:
        sub: Nó <Substation>.

    Returns:
        pd.DataFrame: Tabela de transformadores.
    """
    tr_block = sub.get("Transformers")
    if tr_block is None:
        log_info("[AVISO] Bloco <Transformers> não encontrado em <Substation>.")
        return pd.DataFrame()

    transformers = ensure_list(tr_block.get("Transformer"))
    if not transformers:
        log_info("[AVISO] Nenhum <Transformer> encontrado em <Transformers>.")
        return pd.DataFrame()

    return pd.DataFrame(transformers)


def extract_feeders(sub: Dict[str, Any]) -> pd.DataFrame:
    """
    Extrai os alimentadores (Feeders) da Substation.

    Estrutura esperada:
        Substation
          └── Feeders
                └── Feeder (lista ou único)

    Args:
        sub: Nó <Substation>.

    Returns:
        pd.DataFrame: Tabela de alimentadores.
    """
    feeders_block = sub.get("Feeders")
    if feeders_block is None:
        log_info("[AVISO] Bloco <Feeders> não encontrado em <Substation>.")
        return pd.DataFrame()

    feeders = ensure_list(feeders_block.get("Feeder"))
    if not feeders:
        log_info("[AVISO] Nenhum <Feeder> encontrado em <Feeders>.")
        return pd.DataFrame()

    return pd.DataFrame(feeders)


def extract_loads(sub: Dict[str, Any]) -> pd.DataFrame:
    """
    Extrai as cargas (Loads) da Substation.

    Estrutura esperada:
        Substation
          └── Loads
                └── Load (lista ou único)

    Args:
        sub: Nó <Substation>.

    Returns:
        pd.DataFrame: Tabela de cargas.
    """
    loads_block = sub.get("Loads")
    if loads_block is None:
        log_info("[AVISO] Bloco <Loads> não encontrado em <Substation>.")
        return pd.DataFrame()

    loads = ensure_list(loads_block.get("Load"))
    if not loads:
        log_info("[AVISO] Nenhum <Load> encontrado em <Loads>.")
        return pd.DataFrame()

    return pd.DataFrame(loads)


def extract_breakers(sub: Dict[str, Any]) -> pd.DataFrame:
    """
    Extrai os disjuntores (Breakers) da Substation.

    Estrutura esperada:
        Substation
          └── Breakers
                └── Breaker (lista ou único)

    Args:
        sub: Nó <Substation>.

    Returns:
        pd.DataFrame: Tabela de disjuntores.
    """
    brk_block = sub.get("Breakers")
    if brk_block is None:
        log_info("[AVISO] Bloco <Breakers> não encontrado em <Substation>.")
        return pd.DataFrame()

    breakers = ensure_list(brk_block.get("Breaker"))
    if not breakers:
        log_info("[AVISO] Nenhum <Breaker> encontrado em <Breakers>.")
        return pd.DataFrame()

    return pd.DataFrame(breakers)


# ----------------------------------------------------------------------
# FUNÇÕES DE EXTRAÇÃO — RELÉS / PROTEÇÃO
# ----------------------------------------------------------------------


def extract_relays(root: Dict[str, Any]) -> pd.DataFrame:
    """
    Extrai os relés a partir do nó global <Relays> de <ProtectionDataset>.

    Observação:
        Aqui extraímos apenas a “visão de alto nível” dos relés
        (atributos da tag <Relay>). Estruturas aninhadas como:
        - CTs
        - VTs
        - ProtectionFunctions
        - Parameters
        serão tratadas em detalhe na Fase 2 (NORMALIZE), a partir
        deste CSV bruto ou diretamente do XML, conforme o desenho
        final da N3.

    Estrutura esperada:
        ProtectionDataset
          └── Relays
                └── Relay (lista ou único)

    Args:
        root: Nó <ProtectionDataset>.

    Returns:
        pd.DataFrame: Tabela de relés (nível de cabeçalho).
    """
    relays_root = get_relays_root(root)
    if relays_root is None:
        log_info("[AVISO] Bloco <Relays> não encontrado em <ProtectionDataset>.")
        return pd.DataFrame()

    relays = ensure_list(relays_root.get("Relay"))
    if not relays:
        log_info("[AVISO] Nenhum <Relay> encontrado em <Relays>.")
        return pd.DataFrame()

    # Atenção: colunas que são dicts (CTs, VTs, ProtectionFunctions, Parameters)
    # serão mantidas como estruturas aninhadas. A normalização destes campos
    # será responsabilidade da Fase 2 (NORMALIZE).
    return pd.DataFrame(relays)


# ----------------------------------------------------------------------
# ORQUESTRAÇÃO DA PIPELINE
# ----------------------------------------------------------------------


def run_extract_pipeline() -> None:
    """
    Executa a Fase EXTRACT da pipeline.

    Passos:
        1. Carrega o XML bruto de proteção/topologia.
        2. Localiza o nó raiz <ProtectionDataset>.
        3. Localiza a <Substation> associada.
        4. Extrai entidades principais da Substation:
            - Buses
            - Transformers
            - Feeders
            - Loads
            - Breakers
        5. Extrai os Relays a partir do bloco global <Relays>.
        6. Salva os CSVs em output/csv/.

    Em caso de erro crítico de estrutura (ausência de ProtectionDataset
    ou Substation), a função registra o erro e aborta a execução com
    sys.exit(1).
    """
    log_info("Iniciando Fase EXTRACT...")

    xml_path = "input/xml/refinaria_13k8_protecao_v1.xml"

    if not os.path.exists(xml_path):
        log_error(f"XML não encontrado: {xml_path}")
        sys.exit(1)

    # 1) Carrega e normaliza XML
    data = load_xml(xml_path)

    # 2) Obtém nó raiz <ProtectionDataset>
    root = get_root_dataset(data)

    # 3) Localiza <Substation>
    substation = get_substation(root)

    # 4) Extrações de topologia
    save_csv(extract_buses(substation), "buses")
    save_csv(extract_transformers(substation), "transformers")
    save_csv(extract_feeders(substation), "feeders")
    save_csv(extract_loads(substation), "loads")
    save_csv(extract_breakers(substation), "breakers")

    # 5) Extração de relés (bloco global <Relays>)
    save_csv(extract_relays(root), "relays")

    log_ok("Fase EXTRACT finalizada com sucesso.")


if __name__ == "__main__":
    run_extract_pipeline()
