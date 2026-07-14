"""
App Streamlit - Buscador de Metadados (Unity Catalog + Vector Search)

Frontend para pesquisa semântica de schemas, tabelas e colunas geradas pelos
notebooks deste repositório. A busca usa o índice de Vector Search criado no
Notebook 3 (modelo de entidade) e os metadados detalhados de cada tabela são
lidos diretamente do Unity Catalog via SDK.
"""

import os
import streamlit as st
from databricks.sdk import WorkspaceClient

# =============================================================================
# CONFIGURAÇÃO - preencha de acordo com o que o Notebook 3 imprimiu no final
# =============================================================================
VECTOR_SEARCH_ENDPOINT = os.environ.get("VECTOR_SEARCH_ENDPOINT", "vs_metadados_endpoint")
VECTOR_SEARCH_INDEX = os.environ.get("VECTOR_SEARCH_INDEX", "MEU_CATALOGO.resultados_metadados.tabela_metadados_index")
# Tabela de metadados pesquisável (origem do índice)
METADATA_TABLE = os.environ.get("METADATA_TABLE", "MEU_CATALOGO.resultados_metadados.tabela_metadados")
# =============================================================================

st.set_page_config(page_title="Buscador de Metadados", page_icon="*", layout="wide")


@st.cache_resource
def get_clients():
    return WorkspaceClient()


w = get_clients()

# Colunas do índice (modelo de entidade)
SEARCH_COLUMNS = [
    "id", "entidade", "catalogo", "schema", "tabela", "coluna",
    "nome_completo", "tipo_coluna", "descricao_entidade", "tags",
]


def buscar(query: str, num_results: int = 50):
    """Busca semântica no índice de Vector Search via SDK. Retorna lista de dicts."""
    res = w.vector_search_indexes.query_index(
        index_name=VECTOR_SEARCH_INDEX,
        columns=SEARCH_COLUMNS,
        query_text=query,
        num_results=num_results,
    )
    data = res.result.data_array or []
    cols = [c.name for c in (res.manifest.columns or [])]
    return [dict(zip(cols, r)) for r in data]


def fmt_tags(tags):
    if not tags:
        return ""
    if isinstance(tags, str):
        tags = [tags]
    return " ".join(f"`{t}`" for t in tags)


def get_colunas_uc(full_name: str):
    """Lê os metadados de colunas diretamente do Unity Catalog via SDK."""
    try:
        t = w.tables.get(full_name)
        out = [{"Coluna": c.name, "Tipo": c.type_text, "Comentário": c.comment or ""} for c in (t.columns or [])]
        return out, t
    except Exception as e:
        st.error(f"Erro ao ler {full_name} no Unity Catalog: {e}")
        return [], None


# -----------------------------------------------------------------------------
# UI
# -----------------------------------------------------------------------------
st.title("Buscador de Metadados de Dados")
st.caption("Pesquisa semântica de schemas, tabelas e colunas — incluindo tags — via Vector Search.")

with st.sidebar:
    st.subheader("Configuração")
    st.text(f"Endpoint: {VECTOR_SEARCH_ENDPOINT}")
    st.text(f"Índice: {VECTOR_SEARCH_INDEX}")
    st.text(f"Tabela: {METADATA_TABLE}")
    num_results = st.slider("Máx. resultados", 10, 200, 50, step=10)
    tipos_sel = st.multiselect(
        "Tipos de entidade",
        ["schema", "tabela", "coluna"],
        default=["schema", "tabela", "coluna"],
    )

query = st.text_input(
    "O que você procura?",
    placeholder="Ex.: consumo de energia por cliente, manutenção de transformadores, faturas vencidas...",
)

if query:
    with st.spinner("Buscando..."):
        try:
            rows = buscar(query, num_results=num_results)
        except Exception as e:
            st.error(f"Falha na busca vetorial: {e}")
            rows = []

    rows = [r for r in rows if r.get("entidade") in tipos_sel]

    if not rows:
        st.info("Nenhum resultado encontrado.")
    else:
        schemas = [r for r in rows if r.get("entidade") == "schema"]
        tabelas = [r for r in rows if r.get("entidade") == "tabela"]
        colunas = [r for r in rows if r.get("entidade") == "coluna"]
        st.success(f"{len(rows)} resultado(s): {len(schemas)} schema(s), {len(tabelas)} tabela(s), {len(colunas)} coluna(s).")

        if schemas:
            st.subheader("Schemas")
            for s in schemas:
                st.markdown(f"**`{s.get('nome_completo')}`** — {s.get('descricao_entidade', '')} {fmt_tags(s.get('tags'))}")

        if tabelas:
            st.subheader("Tabelas")
            for t in tabelas:
                full_name = t.get("nome_completo")
                header = f"{t.get('schema', '')}.{t.get('tabela', '')}"
                with st.expander(f"{header}  —  {t.get('descricao_entidade', '')[:120]}"):
                    st.markdown(f"**Nome completo:** `{full_name}`")
                    if t.get("descricao_entidade"):
                        st.markdown(f"**Descrição:** {t['descricao_entidade']}")
                    if t.get("tags"):
                        st.markdown("**Tags:** " + fmt_tags(t.get("tags")))
                    if st.button(f"Ver colunas de {header} (Unity Catalog)", key=f"btn_{full_name}"):
                        cols_uc, tinfo = get_colunas_uc(full_name)
                        if tinfo is not None and tinfo.comment:
                            st.info(f"Comentário UC da tabela: {tinfo.comment}")
                        if cols_uc:
                            st.dataframe(cols_uc, use_container_width=True)

        if colunas:
            st.subheader("Colunas")
            for c in colunas:
                st.markdown(
                    f"- **{c.get('schema')}.{c.get('tabela')}.{c.get('coluna')}** "
                    f"(`{c.get('tipo_coluna')}`): {c.get('descricao_entidade', '')} {fmt_tags(c.get('tags'))}"
                )
else:
    st.info("Digite um termo de busca para começar. A pesquisa cobre nomes, descrições e tags de schemas, tabelas e colunas.")
