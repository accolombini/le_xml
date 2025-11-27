"""
Módulo de funções utilitárias de logging padronizado para toda a pipeline.

Fornece três funções de log:
- log_info(msg)
- log_ok(msg)
- log_error(msg)

O objetivo é centralizar o estilo visual e permitir futura substituição por
logging estruturado, ELK, Prometheus ou arquivos .log, sem alterar scripts.
"""

from datetime import datetime


def _timestamp() -> str:
    """Gera timestamp no formato ISO para os logs."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log_info(msg: str) -> None:
    """Registra mensagem informativa."""
    print(f"[INFO  {_timestamp()}] {msg}")


def log_ok(msg: str) -> None:
    """Registra mensagem de sucesso."""
    print(f"[OK    {_timestamp()}] {msg}")


def log_error(msg: str) -> None:
    """Registra mensagem de erro."""
    print(f"[ERROR {_timestamp()}] {msg}")
