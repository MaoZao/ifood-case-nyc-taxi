"""
Dashboard Streamlit · iFood Case — NYC Yellow Taxi Lakehouse.

Reaproveita a mesma camada de consumo do dashboard estático (index.html):
lê os KPIs de `dashboard/data/kpis.json` (gerado por `analysis/answers.py
--export`) e cai num fallback embutido quando o JSON não existe.

Uso:
    streamlit run dashboard/app.py
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# ----------------------------------------------------------------------------
# Paleta iFood (espelha as variáveis CSS de index.html)
# ----------------------------------------------------------------------------
IFOOD = "#EA1D2C"
IFOOD_DARK = "#B71522"
IFOOD_SOFT = "rgba(234,29,44,.18)"
INK = "#1a1a1f"
MUTED = "#6b7280"
GOOD = "#119C55"
BG = "#f4f5f7"

MESES = {1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr", 5: "Mai"}

DATA_PATH = Path(__file__).parent / "data" / "kpis.json"

# Fallback embutido — mantém o dashboard funcional mesmo sem o pipeline rodado.
EMBEDDED = {
    "meta": {
        "linhas_landing": 811426,
        "linhas_silver": 742636,
        "taxa_descartada_pct": 8.48,
        "periodo": "2023-01 a 2023-05",
        "nota": "Numeros de um SAMPLE sintetico fiel ao schema; "
        "regenere com dados reais via o pipeline.",
    },
    "q1_receita_mensal": [
        {"mes_num": 1, "mes": "Jan", "corridas": 140876, "receita_media_usd": 26.25},
        {"mes_num": 2, "mes": "Fev", "corridas": 133434, "receita_media_usd": 26.70},
        {"mes_num": 3, "mes": "Mar", "corridas": 156030, "receita_media_usd": 27.41},
        {"mes_num": 4, "mes": "Abr", "corridas": 151232, "receita_media_usd": 28.05},
        {"mes_num": 5, "mes": "Mai", "corridas": 161064, "receita_media_usd": 29.00},
    ],
    "q1_media_global_usd": 27.54,
    "q2_passageiros_hora_maio": [
        {"hora": h, "corridas": c, "media_passageiros": p}
        for h, c, p in [
            (0, 2913, 1.619), (1, 1979, 1.634), (2, 1331, 1.716), (3, 990, 1.648),
            (4, 1007, 1.566), (5, 1692, 1.621), (6, 3301, 1.633), (7, 5703, 1.622),
            (8, 8079, 1.626), (9, 8498, 1.634), (10, 7921, 1.644), (11, 8223, 1.651),
            (12, 8564, 1.660), (13, 8668, 1.639), (14, 8799, 1.663), (15, 9619, 1.627),
            (16, 10124, 1.642), (17, 11555, 1.652), (18, 11945, 1.647), (19, 10674, 1.640),
            (20, 9646, 1.642), (21, 8236, 1.635), (22, 6739, 1.646), (23, 4858, 1.643),
        ]
    ],
}


@st.cache_data
def load_data() -> dict:
    """Lê kpis.json e funde com o fallback embutido (JSON sobrescreve)."""
    data = dict(EMBEDDED)
    if DATA_PATH.exists():
        with open(DATA_PATH, encoding="utf-8") as fh:
            data.update(json.load(fh))
    # Garante a normalização de `mes` mesmo quando o JSON traz só mes_num.
    for row in data["q1_receita_mensal"]:
        row.setdefault("mes", MESES.get(row.get("mes_num"), str(row.get("mes_num"))))
    return data


def fmt_usd(v: float) -> str:
    return f"${v:,.2f}"


def fmt_int(v: int) -> str:
    return f"{v:,}".replace(",", ".")


def heat_color(t: float) -> str:
    """Interpola branco -> vermelho iFood, igual ao heatmap do index.html."""
    r = round(255 - (255 - 234) * t)
    g = round(245 - (245 - 29) * t)
    b = round(245 - (245 - 44) * t)
    return f"rgb({r},{g},{b})"


# ----------------------------------------------------------------------------
# Página
# ----------------------------------------------------------------------------
st.set_page_config(
    page_title="iFood · NYC Taxi Lakehouse",
    page_icon="🚕",
    layout="wide",
)

st.markdown(
    f"""
    <style>
      .stApp {{ background:{BG}; }}
      .block-container {{ max-width:1180px; padding-top:1.5rem; }}
      .hero {{
        background:linear-gradient(135deg,{IFOOD} 0%,#FF5A5F 100%);
        color:#fff; border-radius:20px; padding:30px 34px;
        box-shadow:0 14px 40px rgba(234,29,44,.28); margin-bottom:8px;
      }}
      .hero .badge {{
        display:inline-block; background:rgba(255,255,255,.18); padding:5px 12px;
        border-radius:999px; font-size:12px; font-weight:600; letter-spacing:.4px;
      }}
      .hero h1 {{ font-size:30px; font-weight:800; margin:12px 0 6px; letter-spacing:-.5px; }}
      .hero p  {{ opacity:.92; font-size:15px; max-width:680px; margin:0; }}
      .hero .sub {{ display:flex; gap:22px; margin-top:16px; flex-wrap:wrap; font-size:13px; opacity:.95; }}
      .kpi {{
        background:#fff; border:1px solid #e8e9ee; border-radius:16px; padding:18px 20px;
        box-shadow:0 2px 10px rgba(20,20,40,.04); height:100%;
      }}
      .kpi .label {{ font-size:12.5px; color:{MUTED}; font-weight:600;
        text-transform:uppercase; letter-spacing:.5px; }}
      .kpi .value {{ font-size:30px; font-weight:800; margin-top:6px; color:{INK}; }}
      .kpi .value small {{ font-size:15px; color:{MUTED}; font-weight:600; }}
      .kpi .foot {{ font-size:12.5px; margin-top:6px; color:{GOOD}; font-weight:600; }}
      .kpi .foot.neutral {{ color:{MUTED}; }}
      .insight {{
        background:#fff6f6; border-left:4px solid {IFOOD}; border-radius:0 10px 10px 0;
        padding:12px 16px; font-size:13.5px; color:#5a1118; margin-top:10px;
      }}
      .qbadge {{ background:{IFOOD}; color:#fff; font-size:11px; font-weight:800;
        border-radius:6px; padding:2px 7px; letter-spacing:.3px; margin-right:6px; }}
      .tag {{ display:inline-block; background:#eef0f3; border-radius:6px; padding:2px 8px;
        font-size:11px; color:#444; font-weight:600; margin:0 3px; }}
    </style>
    """,
    unsafe_allow_html=True,
)

data = load_data()
meta = data.get("meta", {})
q1 = data["q1_receita_mensal"]
q2 = data["q2_passageiros_hora_maio"]

df1 = pd.DataFrame(q1)
df2 = pd.DataFrame(q2)

avg_global = data.get("q1_media_global_usd", round(df1["receita_media_usd"].mean(), 2))
peak = df1.loc[df1["receita_media_usd"].idxmax()]
total_trips = int(df1["corridas"].sum())
pax_may = (df2["media_passageiros"] * df2["corridas"]).sum() / df2["corridas"].sum()

# ----------------------------------------------------------------------------
# Hero
# ----------------------------------------------------------------------------
linhas_silver = meta.get("linhas_silver", total_trips)
st.markdown(
    f"""
    <div class="hero">
      <span class="badge">CASE TÉCNICO · DATA ARCHITECT</span>
      <h1>NYC Yellow Taxi · Data Lakehouse</h1>
      <p>Ingestão, modelagem e analytics sobre as corridas de táxi de Nova York
         (Jan–Mai 2023) numa arquitetura <b>Medallion</b> com PySpark + Delta Lake.</p>
      <div class="sub">
        <span>🗓️ <b>{meta.get("periodo", "2023-01 a 2023-05")}</b></span>
        <span>🧹 <b>{fmt_int(linhas_silver)}</b> linhas válidas na camada Silver</span>
        <span>🗑️ <b>{meta.get("taxa_descartada_pct", "—")}%</b> descartado na limpeza</span>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# ----------------------------------------------------------------------------
# KPIs
# ----------------------------------------------------------------------------
k1, k2, k3, k4 = st.columns(4)
k1.markdown(
    f'<div class="kpi"><div class="label">Receita média / corrida</div>'
    f'<div class="value">{fmt_usd(avg_global)}</div>'
    f'<div class="foot neutral">média global Jan–Mai</div></div>',
    unsafe_allow_html=True,
)
k2.markdown(
    f'<div class="kpi"><div class="label">Pico de ticket médio</div>'
    f'<div class="value">{fmt_usd(peak["receita_media_usd"])}</div>'
    f'<div class="foot">em {peak["mes"]}</div></div>',
    unsafe_allow_html=True,
)
k3.markdown(
    f'<div class="kpi"><div class="label">Passageiros / corrida (mai)</div>'
    f'<div class="value">{pax_may:.2f} <small>pax</small></div>'
    f'<div class="foot neutral">média do mês de maio</div></div>',
    unsafe_allow_html=True,
)
k4.markdown(
    f'<div class="kpi"><div class="label">Corridas processadas</div>'
    f'<div class="value">{fmt_int(total_trips)}</div>'
    f'<div class="foot neutral">após limpeza de qualidade</div></div>',
    unsafe_allow_html=True,
)

st.write("")

# ----------------------------------------------------------------------------
# Q1 — Receita média por mês + tabela
# ----------------------------------------------------------------------------
col_a, col_b = st.columns([1.25, 1])

with col_a:
    st.markdown(
        '<span class="qbadge">Q1</span><b>Receita média por mês</b>', unsafe_allow_html=True
    )
    st.caption(
        "Média de `total_amount` por corrida, toda a frota yellow. "
        "Linha tracejada = média global."
    )
    fig1 = go.Figure()
    fig1.add_bar(
        x=df1["mes"],
        y=df1["receita_media_usd"],
        marker_color=IFOOD,
        marker_line_width=0,
        hovertemplate="%{x}<br>Receita média: $%{y:.2f}<extra></extra>",
    )
    fig1.add_hline(
        y=avg_global,
        line_dash="dash",
        line_color="#333",
        annotation_text=f"média {fmt_usd(avg_global)}",
        annotation_position="top left",
    )
    fig1.update_layout(
        height=300,
        margin=dict(l=10, r=10, t=10, b=10),
        plot_bgcolor="#fff",
        paper_bgcolor="#fff",
        yaxis=dict(title="US$", tickprefix="$", showgrid=True, gridcolor="#eef0f3"),
        xaxis=dict(showgrid=False),
        showlegend=False,
    )
    st.plotly_chart(fig1, use_container_width=True)

    growth = (df1["receita_media_usd"].iloc[-1] / df1["receita_media_usd"].iloc[0] - 1) * 100
    st.markdown(
        f'<div class="insight">📈 O ticket médio cresceu <b>{growth:.1f}%</b> de '
        f'{df1["mes"].iloc[0]} a {df1["mes"].iloc[-1]} — sinal de sazonalidade/reajuste, '
        f"insumo direto para previsão de receita.</div>",
        unsafe_allow_html=True,
    )

with col_b:
    st.markdown("<b>Volume &amp; ticket</b>", unsafe_allow_html=True)
    st.caption("Tabela-resumo da camada Gold (`agg_receita_mensal`).")
    tbl = df1[["mes", "corridas", "receita_media_usd"]].copy()
    tbl.columns = ["Mês", "Corridas", "Receita média"]
    tbl["Corridas"] = tbl["Corridas"].map(fmt_int)
    tbl["Receita média"] = tbl["Receita média"].map(fmt_usd)
    st.dataframe(tbl, hide_index=True, use_container_width=True)

st.write("")

# ----------------------------------------------------------------------------
# Q2 — Passageiros por hora (combo: barras de demanda + linha de pax)
# ----------------------------------------------------------------------------
st.markdown(
    '<span class="qbadge">Q2</span><b>Passageiros por hora do dia — Maio</b>',
    unsafe_allow_html=True,
)
st.caption(
    "Média de `passenger_count` por hora de embarque (linha) e volume de "
    "corridas como proxy de demanda (barras)."
)

fig2 = go.Figure()
fig2.add_bar(
    x=df2["hora"],
    y=df2["corridas"],
    name="Corridas (demanda)",
    marker_color=IFOOD_SOFT,
    yaxis="y2",
    hovertemplate="%{x}h<br>%{y:,} corridas<extra></extra>",
)
fig2.add_scatter(
    x=df2["hora"],
    y=df2["media_passageiros"],
    name="Média de passageiros",
    mode="lines+markers",
    line=dict(color=IFOOD, width=2.5, shape="spline"),
    fill="tozeroy",
    fillcolor="rgba(234,29,44,.10)",
    yaxis="y",
    hovertemplate="%{x}h<br>%{y:.3f} pax<extra></extra>",
)
fig2.update_layout(
    height=340,
    margin=dict(l=10, r=10, t=30, b=10),
    plot_bgcolor="#fff",
    paper_bgcolor="#fff",
    legend=dict(orientation="h", y=1.12, x=0),
    xaxis=dict(title="hora do dia", dtick=1, showgrid=False),
    yaxis=dict(
        title="passageiros / corrida",
        range=[1.4, 1.8],
        showgrid=True,
        gridcolor="#eef0f3",
    ),
    yaxis2=dict(title="nº de corridas", overlaying="y", side="right", showgrid=False),
)
st.plotly_chart(fig2, use_container_width=True)

h_peak = df2.loc[df2["media_passageiros"].idxmax()]
h_low = df2.loc[df2["media_passageiros"].idxmin()]
st.markdown(
    f'<div class="insight">👥 Ocupação estável (~{pax_may:.2f} pax). Pico às '
    f'<b>{int(h_peak["hora"])}h</b> ({h_peak["media_passageiros"]}) e mínimo às '
    f'<b>{int(h_low["hora"])}h</b> ({h_low["media_passageiros"]}); o volume de corridas '
    f"dispara no fim de tarde — base para dimensionamento de frota e pricing dinâmico.</div>",
    unsafe_allow_html=True,
)

st.write("")

# ----------------------------------------------------------------------------
# Heatmap de demanda 24h
# ----------------------------------------------------------------------------
st.markdown("<b>Mapa de calor da demanda — 24h (Maio)</b>", unsafe_allow_html=True)
st.caption(
    "Intensidade = volume de corridas por hora. Revela os vales da madrugada "
    "e os picos do fim de tarde."
)
colorscale = [[i / 10, heat_color(i / 10)] for i in range(11)]
fig3 = go.Figure(
    go.Heatmap(
        z=[df2["corridas"].tolist()],
        x=[f"{h}h" for h in df2["hora"]],
        y=["demanda"],
        colorscale=colorscale,
        showscale=True,
        colorbar=dict(title="corridas", thickness=12),
        hovertemplate="%{x}<br>%{z:,} corridas<extra></extra>",
        xgap=3,
        ygap=3,
    )
)
fig3.update_layout(
    height=160,
    margin=dict(l=10, r=10, t=10, b=10),
    plot_bgcolor="#fff",
    paper_bgcolor="#fff",
    yaxis=dict(showticklabels=False),
    xaxis=dict(side="bottom"),
)
st.plotly_chart(fig3, use_container_width=True)

# ----------------------------------------------------------------------------
# Rodapé
# ----------------------------------------------------------------------------
st.markdown(
    f'<div style="text-align:center;color:{MUTED};font-size:12px;margin-top:18px">'
    'Camada de consumo: <span class="tag">Delta Lake</span>'
    '<span class="tag">Spark SQL</span><span class="tag">PySpark</span> · '
    f'Dados: NYC TLC Trip Records · {meta.get("nota", "")}</div>',
    unsafe_allow_html=True,
)
