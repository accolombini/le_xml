"""
Módulo normalize.py — Fase 2 da Pipeline (NORMALIZE)

Responsável por:
- Ler o XML bruto da refinaria (mesmo arquivo da Fase EXTRACT)
- Localizar o bloco <Relays> dentro de <ProtectionDataset>
- Explodir e normalizar informações de:
  * Núcleo dos relés (N2)
  * TCs (CTs)
  * TPs (VTs)
  * Funções de proteção (ProtectionFunctions)
  * Settings das funções
  * Curvas e pontos de curva (Curve / CurvePoints)
  * Parâmetros gerais dos relés
  * Relações de seletividade

Saída:
- CSVs normalizados em output/norm_csv/

Autor: Angelo & Assistente
Versão: 1.1 (IDs híbridos e compatíveis com o init.sql)
"""

from __future__ import annotations

import json
import os
import sys
from typing import Any, Dict, List, Optional

import pandas as pd
import xmltodict

from common.logging_utils import log_info, log_ok, log_error


# ---------------------------------------------------------------------------
# UTILITÁRIOS BÁSICOS
# ---------------------------------------------------------------------------


def ensure_list(obj: Any) -> List[Any]:
    """Garante que o objeto seja sempre uma lista."""
    if obj is None:
        return []
    return obj if isinstance(obj, list) else [obj]


def strip_namespaces(data: Any) -> Any:
    """Remove namespaces de todas as chaves do dicionário XML."""
    if isinstance(data, dict):
        new_dict: Dict[str, Any] = {}
        for key, value in data.items():
            clean_key = key.split(":")[-1]
            new_dict[clean_key] = strip_namespaces(value)
        return new_dict

    if isinstance(data, list):
        return [strip_namespaces(item) for item in data]

    return data


def load_xml(path: str) -> Dict[str, Any]:
    """Carrega o arquivo XML, remove namespaces e devolve um dicionário limpo."""
    log_info(f"Lendo arquivo XML: {path}")

    try:
        with open(path, "r", encoding="utf-8") as file:
            raw_content = file.read()
    except OSError as exc:
        log_error(f"Erro ao abrir XML: {exc}")
        sys.exit(1)

    try:
        parsed = xmltodict.parse(raw_content)
        cleaned = strip_namespaces(parsed)
        log_ok("XML carregado e normalizado (namespaces removidos).")
        return cleaned
    except Exception as exc:  # noqa: BLE001
        log_error(f"Erro ao interpretar XML: {exc}")
        sys.exit(1)


def get_relays_list(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Localiza a lista de relés em ProtectionDataset/Relays/Relay.
    """
    root = data.get("ProtectionDataset")
    if root is None:
        log_error("Nó raiz <ProtectionDataset> não encontrado no XML.")
        sys.exit(1)

    relays_root = root.get("Relays")
    if relays_root is None:
        log_error("Nó <Relays> não encontrado dentro de <ProtectionDataset>.")
        sys.exit(1)

    relays = relays_root.get("Relay")
    if relays is None:
        log_error("Nenhum nó <Relay> encontrado dentro de <Relays>.")
        sys.exit(1)

    relay_list = ensure_list(relays)
    log_ok(f"Encontrados {len(relay_list)} relés no XML.")
    return relay_list


def ensure_norm_dir() -> str:
    """Garante a existência do diretório de saída N2/N3."""
    norm_dir = os.path.join("output", "norm_csv")
    os.makedirs(norm_dir, exist_ok=True)
    return norm_dir


def save_norm_csv(df: pd.DataFrame, name: str) -> None:
    """Salva um DataFrame como CSV dentro de output/norm_csv."""
    norm_dir = ensure_norm_dir()
    out_path = os.path.join(norm_dir, f"{name}.csv")

    if df.empty:
        log_info(f"Nenhum registro para {name}.csv — arquivo não será gerado.")
        return

    df.to_csv(out_path, index=False)
    log_ok(f"Arquivo normalizado gerado: {out_path}")


# ---------------------------------------------------------------------------
# POLÍTICA DE IDs HÍBRIDA (XML → DB)
# ---------------------------------------------------------------------------

# Contadores simples por tipo de entidade para geração de IDs sintéticos
_ID_COUNTERS: Dict[str, int] = {
    "VT": 0,
    "SETTING": 0,
    "CURVE": 0,
    "POINT": 0,
    "PARAM": 0,
    "SEL": 0,
}


def _next_counter(kind: str) -> int:
    """Retorna o próximo contador para um determinado tipo de ID."""
    _ID_COUNTERS[kind] = _ID_COUNTERS.get(kind, 0) + 1
    return _ID_COUNTERS[kind]


def make_vt_id(
    relay_id: Optional[str],
    vt_xml_id: Optional[str],
    idx: int,
) -> str:
    """
    Política de ID para VT:

    1) Se o XML tiver @id, usamos esse valor.
    2) Caso contrário, usamos um ID sintético estável:
       VT-{relay_id}-{idx:02d} (se relay_id existir)
       ou VT-AUTO-{global_counter:06d}
    """
    if vt_xml_id:
        return vt_xml_id

    if relay_id:
        return f"VT-{relay_id}-{idx:02d}"

    n = _next_counter("VT")
    return f"VT-AUTO-{n:06d}"


def make_curve_id(
    relay_id: Optional[str],
    func_id: Optional[str],
    curve_xml_id: Optional[str],
    idx: int,
) -> str:
    """
    Política de ID para Curves:

    1) Se o XML tiver @id, usamos esse valor.
    2) Caso contrário, usamos CUR-{relay_id}-{func_id}-{idx:02d}
       (quando relay_id/func_id existem).
    3) Se nem relay_id nem func_id estiverem presentes, usamos um ID global.
    """
    if curve_xml_id:
        return curve_xml_id

    if relay_id and func_id:
        return f"CUR-{relay_id}-{func_id}-{idx:02d}"

    n = _next_counter("CURVE")
    return f"CUR-AUTO-{n:06d}"


def make_point_id(curve_id: str, idx: int) -> str:
    """
    Política de ID para CurvePoints:

    - Sempre sintético, mas determinístico por curva e ordem.
    """
    return f"PT-{curve_id}-{idx:03d}"


def make_setting_id(relay_id: str, func_id: str) -> str:
    """
    Política de ID para Function Settings:

    - Um registro agregado por função:
      SET-{relay_id}-{func_id}
    """
    return f"SET-{relay_id}-{func_id}"


def make_parameter_id(relay_id: str, idx: int) -> str:
    """
    Política de ID para Parameters:

    - Sempre sintético, mas determinístico por relé e ordem:
      PAR-{relay_id}-{idx:03d}
    """
    return f"PAR-{relay_id}-{idx:03d}"


def make_selectivity_id(relay_id: str, func_id: str, direction: str, idx: int) -> str:
    """
    Política de ID para Selectivity:

    - SEL-{relay_id}-{func_id}-{D/U}-{idx:03d}
    """
    dir_code = "D" if direction.lower().startswith("down") else "U"
    return f"SEL-{relay_id}-{func_id}-{dir_code}-{idx:03d}"


def safe_float(value: Optional[str]) -> Optional[float]:
    """
    Converte valores para float de forma segura.

    Trata casos como:
    - None → None
    - "NaN", "" → None
    - "∞" ou "INF" → None (pontos assintóticos não numéricos)
    """
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()

    if text == "" or text.lower() == "nan":
        return None

    if text in {"∞", "+∞", "-∞", "inf", "+inf", "-inf"}:
        return None

    try:
        return float(text)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# NORMALIZAÇÃO N2 — NÚCLEO DOS RELÉS
# ---------------------------------------------------------------------------


def normalize_relays_core(relays: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Normaliza o núcleo (N2) dos relés.

    Colunas típicas:
    - relay_id, manufacturer, model, series, relay_type
    - voltageclass_kv, frequency_hz, config_date
    - protected_transformer_id, protected_feeder_id, protected_load_id
    """
    rows: List[Dict[str, Any]] = []

    for relay in relays:
        rows.append(
            {
                "relay_id": relay.get("@id"),
                "manufacturer": relay.get("@manufacturer"),
                "model": relay.get("@model"),
                "series": relay.get("@series"),
                "relay_type": relay.get("@relayType"),
                "voltage_class_kv": relay.get("@voltageClassKV"),
                "frequency_hz": safe_float(relay.get("@frequencyHz")),
                "config_date": relay.get("@configDate"),
                "protected_transformer_id": relay.get("@protectedTransformerId"),
                "protected_feeder_id": relay.get("@protectedFeederId"),
                "protected_load_id": relay.get("@protectedLoadId"),
            },
        )

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# NORMALIZAÇÃO N3 — CTs, VTs, FUNÇÕES, CURVAS, PARÂMETROS
# ---------------------------------------------------------------------------


def normalize_cts(relays: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Explode e normaliza os TCs (CTs) por relé.
    """
    rows: List[Dict[str, Any]] = []

    for relay in relays:
        relay_id = relay.get("@id")
        cts_block = relay.get("CTs")

        if not cts_block:
            continue

        for idx, ct in enumerate(ensure_list(cts_block.get("CT")), start=1):
            if not isinstance(ct, dict):
                continue

            primary = safe_float(ct.get("@primaryA"))
            secondary = safe_float(ct.get("@secondaryA"))

            try:
                ratio = (primary / secondary) if primary and secondary else None
            except Exception:  # noqa: BLE001
                ratio = None

            ct_id = ct.get("@id") or f"CT-{relay_id}-{idx:02d}"

            rows.append(
                {
                    "relay_id": relay_id,
                    "ct_id": ct_id,
                    "location": ct.get("@location"),
                    "phase": ct.get("@phase"),
                    "primary_a": primary,
                    "secondary_a": secondary,
                    "ratio": ratio,
                    "class": ct.get("@class"),
                    "burden_va": safe_float(ct.get("@burdenVA")),
                    "core_id": ct.get("@coreId"),
                },
            )

    return pd.DataFrame(rows)


def normalize_vts(relays: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Explode e normaliza os TPs (VTs) por relé.

    Observação:
    - Casos sem VT explícito, mas com vtDefined/vtEnabled, recebem um
      VT sintético com ID estável.
    """
    rows: List[Dict[str, Any]] = []

    for relay in relays:
        relay_id = relay.get("@id")
        vts_block = relay.get("VTs")

        if not vts_block:
            continue

        vt_defined = vts_block.get("@vtDefined")
        vt_enabled = vts_block.get("@vtEnabled")
        vt_nodes = vts_block.get("VT")

        if vt_nodes is None:
            # Só flags de VT, sem VT explícito
            vt_id = make_vt_id(relay_id=relay_id, vt_xml_id=None, idx=1)
            rows.append(
                {
                    "relay_id": relay_id,
                    "vt_id": vt_id,
                    "location": None,
                    "primary_kv": None,
                    "secondary_v": None,
                    "connection": None,
                    "burden_va": None,
                    "vt_defined": vt_defined,
                    "vt_enabled": vt_enabled,
                },
            )
            continue

        for idx, vt in enumerate(ensure_list(vt_nodes), start=1):
            if not isinstance(vt, dict):
                continue

            vt_id = make_vt_id(
                relay_id=relay_id,
                vt_xml_id=vt.get("@id"),
                idx=idx,
            )

            rows.append(
                {
                    "relay_id": relay_id,
                    "vt_id": vt_id,
                    "location": vt.get("@location"),
                    "primary_kv": safe_float(vt.get("@primaryKV")),
                    "secondary_v": safe_float(vt.get("@secondaryV")),
                    "connection": vt.get("@connection"),
                    "burden_va": safe_float(vt.get("@burdenVA")),
                    "vt_defined": vt_defined,
                    "vt_enabled": vt_enabled,
                },
            )

    return pd.DataFrame(rows)


def normalize_functions(relays: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Normaliza o cabeçalho das funções de proteção.

    Cada linha representa uma Function:
    - relay_id, function_id, name, ansi_code, enabled,
      zone, directionality, trip_output, ct_ref
    """
    rows: List[Dict[str, Any]] = []

    for relay in relays:
        relay_id = relay.get("@id")
        pf_block = relay.get("ProtectionFunctions")

        if not pf_block:
            continue

        for func in ensure_list(pf_block.get("Function")):
            if not isinstance(func, dict):
                continue

            rows.append(
                {
                    "relay_id": relay_id,
                    "function_id": func.get("@id"),
                    "name": func.get("@name"),
                    "ansi_code": func.get("@ansiCode"),
                    "enabled": func.get("@enabled"),
                    "zone": func.get("@zone"),
                    "directionality": func.get("@directionality"),
                    "trip_output": func.get("@tripOutput"),
                    "ct_ref": func.get("@ctRef"),
                },
            )

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# FUNCTION SETTINGS (AGREGADO POR FUNÇÃO)
# ---------------------------------------------------------------------------


def _collect_raw_settings_for_function(func: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Coleta settings "crus" de uma função (parameter/attribute/value).

    É a mesma lógica do normalize antigo, mas restrita a uma função,
    para depois agregarmos em uma linha por função.
    """
    rows: List[Dict[str, Any]] = []
    settings = func.get("Settings")

    if not settings:
        return rows

    for key, node in settings.items():
        # O nó Curve é tratado separadamente
        if key == "Curve":
            continue

        # Caso o nodo seja apenas um texto simples
        if not isinstance(node, dict):
            rows.append(
                {
                    "parameter": key,
                    "attribute": "_text",
                    "value": str(node),
                },
            )
            continue

        # Caso típico: dict com atributos @...
        for attr_name, attr_val in node.items():
            if not attr_name.startswith("@"):
                continue
            rows.append(
                {
                    "parameter": key,
                    "attribute": attr_name.lstrip("@"),
                    "value": attr_val,
                },
            )

    return rows


def normalize_function_settings(relays: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Normaliza os parâmetros (Settings) de cada função de proteção
    em formato AGREGADO, compatível com a tabela relays_function_settings:

    - setting_id (PK)
    - function_id (FK)
    - pickup_pu
    - pickup_a
    - time_dial
    - min_time_seconds
    - thermal_constant
    - full_load_current
    - trip_class
    - extra_json  (restante dos parâmetros)
    """
    rows: List[Dict[str, Any]] = []

    for relay in relays:
        relay_id = relay.get("@id")
        pf_block = relay.get("ProtectionFunctions")

        if not pf_block:
            continue

        for func in ensure_list(pf_block.get("Function")):
            if not isinstance(func, dict):
                continue

            func_id = func.get("@id") or ""
            raw_settings = _collect_raw_settings_for_function(func)

            if not raw_settings:
                continue

            # Mapa parameter -> list[ {attribute, value} ]
            by_param: Dict[str, List[Dict[str, Any]]] = {}
            for item in raw_settings:
                param = item["parameter"]
                by_param.setdefault(param, []).append(
                    {
                        "attribute": item["attribute"],
                        "value": item["value"],
                    },
                )

            # Campos principais (tentamos mapear por nomes usuais)
            pickup_pu: Optional[float] = None
            pickup_a: Optional[float] = None
            time_dial: Optional[float] = None
            min_time_seconds: Optional[float] = None
            thermal_constant: Optional[float] = None
            full_load_current: Optional[float] = None
            trip_class: Optional[float] = None

            extra: Dict[str, Any] = {}

            for param_name, items in by_param.items():
                for item in items:
                    attribute = item["attribute"]
                    value = item["value"]

                    key = f"{param_name}.{attribute}"
                    extra[key] = value

                    pname_lower = param_name.lower()

                    if pname_lower in {"pickupperunit", "pickup_per_unit", "pickup_pu"}:
                        pickup_pu = safe_float(value)
                    elif pname_lower in {"pickupamps", "pickup_a", "pickup_current"}:
                        pickup_a = safe_float(value)
                    elif pname_lower in {"timedial", "time_dial"}:
                        time_dial = safe_float(value)
                    elif pname_lower in {
                        "mintimeseconds",
                        "min_time_seconds",
                        "min_time",
                    }:
                        min_time_seconds = safe_float(value)
                    elif pname_lower in {
                        "thermalconstant",
                        "thermal_constant",
                    }:
                        thermal_constant = safe_float(value)
                    elif pname_lower in {
                        "fullloadcurrent",
                        "full_load_current",
                        "fla",
                    }:
                        full_load_current = safe_float(value)
                    elif pname_lower in {"tripclass", "trip_class"}:
                        # Trip class muitas vezes é número discreto (10, 20, 30)
                        trip_class = safe_float(value)

            setting_id = make_setting_id(relay_id=relay_id, func_id=func_id)

            rows.append(
                {
                    "setting_id": setting_id,
                    "function_id": func_id,
                    "pickup_pu": pickup_pu,
                    "pickup_a": pickup_a,
                    "time_dial": time_dial,
                    "min_time_seconds": min_time_seconds,
                    "thermal_constant": thermal_constant,
                    "full_load_current": full_load_current,
                    "trip_class": trip_class,
                    "extra_json": json.dumps(extra, ensure_ascii=False)
                    if extra
                    else None,
                },
            )

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# CURVES — METADADOS
# ---------------------------------------------------------------------------


def normalize_function_curves(relays: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Normaliza os metadados das curvas de proteção.

    Colunas principais (alinhadas ao init.sql):
    - curve_id (PK)
    - function_id (FK)
    - family
    - type
    - standard
    - pickup_pu
    - pickup_a
    - time_dial
    - min_time_sec
    - parametric
    - extra_json (opcional)
    """
    rows: List[Dict[str, Any]] = []

    for relay in relays:
        relay_id = relay.get("@id")
        pf_block = relay.get("ProtectionFunctions")

        if not pf_block:
            continue

        for func in ensure_list(pf_block.get("Function")):
            if not isinstance(func, dict):
                continue

            func_id = func.get("@id")
            curves_nodes: List[Dict[str, Any]] = []

            settings = func.get("Settings")
            if settings and "Curve" in settings:
                curves_nodes.extend(ensure_list(settings["Curve"]))

            if "Curve" in func:
                curves_nodes.extend(ensure_list(func["Curve"]))

            for idx, curve in enumerate(curves_nodes, start=1):
                if not isinstance(curve, dict):
                    continue

                # Atributos do nó Curve
                attrs = {k.lstrip("@"): v for k, v in curve.items() if k.startswith("@")}

                curve_xml_id = attrs.get("id")
                curve_id = make_curve_id(
                    relay_id=relay_id,
                    func_id=func_id,
                    curve_xml_id=curve_xml_id,
                    idx=idx,
                )

                family = attrs.pop("family", None)
                curve_type = attrs.pop("type", None)
                standard = attrs.pop("standard", None)

                pickup_pu = safe_float(attrs.pop("pickupPU", None))
                pickup_a = safe_float(attrs.pop("pickupA", None))
                time_dial = safe_float(attrs.pop("timeDial", None))
                min_time_sec = safe_float(attrs.pop("minTimeSeconds", None))

                parametric_raw = attrs.pop("parametric", None)
                parametric: Optional[bool] = None
                if isinstance(parametric_raw, str):
                    if parametric_raw.lower() in {"true", "1", "yes"}:
                        parametric = True
                    elif parametric_raw.lower() in {"false", "0", "no"}:
                        parametric = False

                extra_json = json.dumps(attrs, ensure_ascii=False) if attrs else None

                rows.append(
                    {
                        "curve_id": curve_id,
                        "function_id": func_id,
                        "family": family,
                        "type": curve_type,
                        "standard": standard,
                        "pickup_pu": pickup_pu,
                        "pickup_a": pickup_a,
                        "time_dial": time_dial,
                        "min_time_sec": min_time_sec,
                        "parametric": parametric,
                        "extra_json": extra_json,
                    },
                )

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# CURVE POINTS — PONTOS DAS CURVAS
# ---------------------------------------------------------------------------


def normalize_curve_points(relays: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Normaliza os pontos de cada curva (CurvePoints).

    Colunas alinhadas ao init.sql:
    - point_id (PK)
    - curve_id (FK)
    - base
    - multiple
    - time_seconds
    - amps
    - volts
    """
    rows: List[Dict[str, Any]] = []

    for relay in relays:
        relay_id = relay.get("@id")
        pf_block = relay.get("ProtectionFunctions")

        if not pf_block:
            continue

        for func in ensure_list(pf_block.get("Function")):
            if not isinstance(func, dict):
                continue

            func_id = func.get("@id")

            # Precisamos inferir o mesmo curve_id usado em normalize_function_curves
            # Estratégia: se houver Curve com @id, usamos esse ID.
            # Caso contrário, aplicamos a mesma regra: CUR-{relay_id}-{func_id}-01
            curves_nodes = []
            settings = func.get("Settings")
            if settings and "Curve" in settings:
                curves_nodes.extend(ensure_list(settings["Curve"]))
            if "Curve" in func:
                curves_nodes.extend(ensure_list(func["Curve"]))

            # Por simplicidade (caso POC): assumimos a primeira curva
            if curves_nodes:
                first_curve = curves_nodes[0]
                if isinstance(first_curve, dict):
                    attrs = {
                        k.lstrip("@"): v
                        for k, v in first_curve.items()
                        if k.startswith("@")
                    }
                    curve_xml_id = attrs.get("id")
                else:
                    curve_xml_id = None
            else:
                curve_xml_id = None

            curve_id = make_curve_id(
                relay_id=relay_id,
                func_id=func_id,
                curve_xml_id=curve_xml_id,
                idx=1,
            )

            cp_block = func.get("CurvePoints")
            if not cp_block:
                continue

            base = cp_block.get("@base")

            for idx, point in enumerate(ensure_list(cp_block.get("Point")), start=1):
                if not isinstance(point, dict):
                    continue

                attrs = {
                    k.lstrip("@"): v for k, v in point.items() if k.startswith("@")
                }

                multiple = safe_float(attrs.get("multiple"))
                current = safe_float(attrs.get("current"))
                time_seconds = safe_float(attrs.get("timeSeconds"))

                point_id = make_point_id(curve_id=curve_id, idx=idx)

                rows.append(
                    {
                        "point_id": point_id,
                        "curve_id": curve_id,
                        "base": base,
                        "multiple": multiple,
                        "time_seconds": time_seconds,
                        "amps": current,
                        "volts": None,
                    },
                )

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# PARAMETERS
# ---------------------------------------------------------------------------


def normalize_parameters(relays: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Normaliza os parâmetros gerais de cada relé (<Parameters>).

    Colunas:
    - parameter_id (PK)
    - relay_id
    - name
    - group_name
    - type
    - value
    """
    rows: List[Dict[str, Any]] = []

    for relay in relays:
        relay_id = relay.get("@id")
        params_block = relay.get("Parameters")

        if not params_block:
            continue

        for idx, param in enumerate(
            ensure_list(params_block.get("Parameter")),
            start=1,
        ):
            if not isinstance(param, dict):
                continue

            parameter_id = param.get("@id") or make_parameter_id(
                relay_id=relay_id,
                idx=idx,
            )

            rows.append(
                {
                    "parameter_id": parameter_id,
                    "relay_id": relay_id,
                    "name": param.get("@name"),
                    "group_name": param.get("@group"),
                    "type": param.get("@type"),
                    "value": param.get("@value"),
                },
            )

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# SELECTIVITY
# ---------------------------------------------------------------------------


def normalize_selectivity(relays: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Normaliza as relações de seletividade entre funções de proteção.

    Origem:
    - Bloco <Selectivity> dentro de cada <Function>

    Colunas:
    - selectivity_id (PK)
    - function_id
    - upstream_device
    - downstream_device
    - element
    - coordination_margin
    """
    rows: List[Dict[str, Any]] = []

    for relay in relays:
        relay_id = relay.get("@id")
        pf_block = relay.get("ProtectionFunctions")

        if not pf_block:
            continue

        for func in ensure_list(pf_block.get("Function")):
            if not isinstance(func, dict):
                continue

            func_id = func.get("@id")
            sel_block = func.get("Selectivity")

            if not sel_block:
                continue

            margin = sel_block.get("CoordinationMargin", {})
            margin_s = safe_float(margin.get("@seconds"))

            # Downstream
            for idx, dev in enumerate(
                ensure_list(sel_block.get("DownstreamDevice")),
                start=1,
            ):
                if not isinstance(dev, dict):
                    continue

                selectivity_id = make_selectivity_id(
                    relay_id=relay_id,
                    func_id=func_id,
                    direction="Downstream",
                    idx=idx,
                )

                rows.append(
                    {
                        "selectivity_id": selectivity_id,
                        "function_id": func_id,
                        "upstream_device": None,
                        "downstream_device": dev.get("@id"),
                        "element": dev.get("@element"),
                        "coordination_margin": margin_s,
                    },
                )

            # Upstream
            for idx, dev in enumerate(
                ensure_list(sel_block.get("UpstreamDevice")),
                start=1,
            ):
                if not isinstance(dev, dict):
                    continue

                selectivity_id = make_selectivity_id(
                    relay_id=relay_id,
                    func_id=func_id,
                    direction="Upstream",
                    idx=idx,
                )

                rows.append(
                    {
                        "selectivity_id": selectivity_id,
                        "function_id": func_id,
                        "upstream_device": dev.get("@id"),
                        "downstream_device": None,
                        "element": dev.get("@element"),
                        "coordination_margin": margin_s,
                    },
                )

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# ORQUESTRAÇÃO DA FASE NORMALIZE
# ---------------------------------------------------------------------------


def run_normalize_pipeline() -> None:
    """
    Executa a Fase 2 (NORMALIZE) da pipeline.

    Etapas:
        1. Carrega o XML bruto (mesmo usado em EXTRACT).
        2. Localiza e carrega a lista de relés.
        3. Gera DataFrames normalizados (N2 / N3).
        4. Salva CSVs em output/norm_csv/.
    """
    log_info("Iniciando Fase NORMALIZE...")

    xml_path = "input/xml/refinaria_13k8_protecao_v1.xml"
    if not os.path.exists(xml_path):
        log_error(f"XML não encontrado: {xml_path}")
        sys.exit(1)

    data = load_xml(xml_path)
    relays = get_relays_list(data)

    # N2 — núcleo dos relés
    save_norm_csv(normalize_relays_core(relays), "relays_core")

    # N3 — detalhes
    save_norm_csv(normalize_cts(relays), "relays_cts")
    save_norm_csv(normalize_vts(relays), "relays_vts")
    save_norm_csv(normalize_functions(relays), "relays_functions")
    save_norm_csv(normalize_function_settings(relays), "relays_function_settings")
    save_norm_csv(normalize_function_curves(relays), "relays_curves")
    save_norm_csv(normalize_curve_points(relays), "relays_curve_points")
    save_norm_csv(normalize_parameters(relays), "relays_parameters")
    save_norm_csv(normalize_selectivity(relays), "relays_selectivity")

    log_ok("Fase NORMALIZE finalizada com sucesso.")


if __name__ == "__main__":
    run_normalize_pipeline()
