"""Dashboard operacional — le a camada gold via DuckDB.

Nenhum dado e reprocessado aqui: o Streamlit apenas consulta as VIEWs do
catalogo DuckDB montado pela etapa `lakehouse` do pipeline.
"""
from __future__ import annotations

import duckdb
import plotly.express as px
import streamlit as st

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from src.config import load_config  # noqa: E402

st.set_page_config(page_title="Frota • Painel Operacional", layout="wide")
cfg = load_config()


@st.cache_resource
def _con() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(cfg.duckdb_path), read_only=True)


@st.cache_data(ttl=300)
def q(sql: str):
    return _con().execute(sql).df()


st.title("Painel Operacional da Frota")
st.caption("Dados tratados na camada gold (PySpark) e servidos via DuckDB.")

if not cfg.duckdb_path.exists():
    st.error(
        f"Catalogo DuckDB nao encontrado em {cfg.duckdb_path}. "
        "Rode o pipeline antes (`python -m src.pipeline` ou `docker compose up`)."
    )
    st.stop()

# ---- KPIs -----------------------------------------------------------------
total = q("SELECT COUNT(*) n, SUM(CASE WHEN status='concluida' THEN 1 ELSE 0 END) c FROM viagens_enriquecidas")
atraso = q("SELECT SUM(viagens_atrasadas) a, SUM(total_viagens) t FROM taxa_atraso_por_mes")
parado = q("SELECT ROUND(AVG(tempo_medio_parado_min),1) m FROM tempo_medio_parado_por_tipo")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Viagens", f"{int(total['n'][0]):,}".replace(",", "."))
c2.metric("Concluidas", f"{int(total['c'][0]):,}".replace(",", "."))
taxa = (atraso["a"][0] / atraso["t"][0]) if atraso["t"][0] else 0
c3.metric("Taxa de atraso", f"{taxa*100:.1f}%")
c4.metric("Tempo medio parado", f"{parado['m'][0]:.0f} min")

st.info(
    "Observacoes sobre a base (fieis aos dados, nao ajustadas):\n"
    "- As viagens concentram-se em um unico mes (2026-04), entao as series "
    "mensais tem um ponto so.\n"
    "- **Tempo medio parado ~ 0**: cada passagem por geocerca tem um unico ponto "
    "de GPS registrado, entao nao ha permanencia mensuravel.\n"
    "- **Utilizacao 100%**: todos os veiculos ativos realizaram viagens no periodo."
)

st.divider()

# ---- Viagens por mes e status --------------------------------------------
col1, col2 = st.columns(2)
with col1:
    st.subheader("Viagens por mes e status")
    df = q("SELECT mes_referencia, status, qtd_viagens FROM viagens_por_mes_status ORDER BY mes_referencia")
    fig = px.bar(df, x="mes_referencia", y="qtd_viagens", color="status", barmode="stack")
    fig.update_xaxes(type="category")
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Taxa de atraso por mes")
    df = q("SELECT mes_referencia, taxa_atraso FROM taxa_atraso_por_mes ORDER BY mes_referencia")
    fig = px.bar(df, x="mes_referencia", y="taxa_atraso")
    fig.update_xaxes(type="category")
    fig.update_yaxes(tickformat=".0%")
    st.plotly_chart(fig, use_container_width=True)

# ---- Top motoristas + utilizacao frota -----------------------------------
col3, col4 = st.columns(2)
with col3:
    st.subheader("Top 10 motoristas (viagens concluidas)")
    df = q("SELECT nome_motorista, viagens_concluidas FROM top10_motoristas ORDER BY viagens_concluidas")
    fig = px.bar(df, x="viagens_concluidas", y="nome_motorista", orientation="h")
    st.plotly_chart(fig, use_container_width=True)

with col4:
    st.subheader("Utilizacao da frota por mes")
    df = q("SELECT mes_referencia, taxa_utilizacao FROM utilizacao_frota_por_mes ORDER BY mes_referencia")
    fig = px.bar(df, x="mes_referencia", y="taxa_utilizacao")
    fig.update_xaxes(type="category")
    fig.update_yaxes(tickformat=".0%")
    st.plotly_chart(fig, use_container_width=True)

# ---- Tempo parado por tipo + mapa ----------------------------------------
col5, col6 = st.columns(2)
with col5:
    st.subheader("Tempo medio parado por tipo de geocerca")
    df = q(
        "SELECT tipo_geocerca, tempo_medio_parado_min, qtd_visitas "
        "FROM tempo_medio_parado_por_tipo ORDER BY qtd_visitas DESC"
    )
    st.dataframe(df, use_container_width=True, hide_index=True)
    st.caption(
        "Valor ~0: cada passagem por geocerca tem um unico ponto de GPS registrado, "
        "entao nao ha permanencia mensuravel (fiel a base)."
    )

with col6:
    st.subheader("Posicoes GPS (amostra)")
    df = q(
        "SELECT latitude, longitude, classificacao FROM posicoes_geo USING SAMPLE 5000 ROWS"
    )
    fig = px.scatter_mapbox(
        df,
        lat="latitude",
        lon="longitude",
        color="classificacao",
        zoom=3,
        height=400,
    )
    fig.update_layout(mapbox_style="open-street-map", margin=dict(l=0, r=0, t=0, b=0))
    st.plotly_chart(fig, use_container_width=True)

st.divider()
st.subheader("Tempo medio por rota (top 15)")
st.dataframe(
    q(
        "SELECT origem_nome, destino_nome, tempo_medio_horas, qtd_viagens "
        "FROM tempo_medio_por_rota ORDER BY qtd_viagens DESC LIMIT 15"
    ),
    use_container_width=True,
    hide_index=True,
)

st.divider()
st.subheader("Qualidade dos dados — quarentena")
st.caption(
    "Registros descartados na limpeza sao rastreados (nao apagados), com o motivo."
)
qc1, qc2 = st.columns(2)
with qc1:
    st.markdown("**Viagens rejeitadas por motivo**")
    st.dataframe(
        q("SELECT motivo_rejeicao, COUNT(*) AS qtd FROM rejeitados_viagens GROUP BY 1 ORDER BY qtd DESC"),
        use_container_width=True,
        hide_index=True,
    )
with qc2:
    st.markdown("**Posicoes rejeitadas por motivo**")
    st.dataframe(
        q("SELECT motivo_rejeicao, COUNT(*) AS qtd FROM rejeitados_posicoes GROUP BY 1 ORDER BY qtd DESC"),
        use_container_width=True,
        hide_index=True,
    )
