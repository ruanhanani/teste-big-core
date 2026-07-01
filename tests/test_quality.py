"""Testes do validador de CPF (regra de negocio nao trivial)."""
from src.quality import _cpf_valido


def test_cpf_valido_aceita_validos():
    assert _cpf_valido("529.982.247-25")
    assert _cpf_valido("52998224725")


def test_cpf_valido_rejeita_invalidos():
    assert not _cpf_valido("111.111.111-11")  # todos iguais
    assert not _cpf_valido("529.982.247-24")  # digito verificador errado
    assert not _cpf_valido("123")             # tamanho errado
    assert not _cpf_valido("")                # vazio
    assert not _cpf_valido(None)              # nulo
