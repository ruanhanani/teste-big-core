"""Testes do point-in-polygon (nucleo do enriquecimento geoespacial)."""
from shapely.geometry import Point

from src import geo


def _quadrado_wkt():
    # quadrado simples de (0,0) a (10,10)
    return "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"


def setup_function():
    geo._TREE_CACHE.clear()


def test_ponto_dentro_da_geocerca():
    tree, ids = geo.build_tree([("GEO-0001", _quadrado_wkt())])
    assert geo.match_point(tree, ids, Point(5, 5)) == "GEO-0001"


def test_ponto_fora_de_qualquer_geocerca():
    tree, ids = geo.build_tree([("GEO-0001", _quadrado_wkt())])
    assert geo.match_point(tree, ids, Point(20, 20)) is None
