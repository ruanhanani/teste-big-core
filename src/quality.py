"""Validadores de qualidade reutilizaveis (UDFs de negocio)."""
from __future__ import annotations

import re

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

_PLACA_MERCOSUL = re.compile(r"^[A-Z]{3}[0-9][A-Z0-9][0-9]{2}$")


def _cpf_valido(cpf: str | None) -> bool:
    """Valida CPF pelos digitos verificadores (ignora mascara)."""
    if not cpf:
        return False
    digits = re.sub(r"\D", "", cpf)
    if len(digits) != 11 or digits == digits[0] * 11:
        return False
    for i in (9, 10):
        soma = sum(int(digits[n]) * ((i + 1) - n) for n in range(i))
        dv = (soma * 10) % 11 % 10
        if dv != int(digits[i]):
            return False
    return True


cpf_valido_udf = F.udf(_cpf_valido, BooleanType())


def placa_valida(coluna: str):
    """Coluna booleana indicando placa no padrao Mercosul (LLLNLNN)."""
    return F.upper(F.col(coluna)).rlike(_PLACA_MERCOSUL.pattern)
