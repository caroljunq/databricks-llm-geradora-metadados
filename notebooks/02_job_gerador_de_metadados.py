# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 2 - Job Gerador de Metadados
# MAGIC
# MAGIC Obtém os metadados do schema `demo_energia` diretamente do Unity Catalog (via SDK)
# MAGIC e usa o endpoint `databricks-meta-llama-3-1-8b-instruct` para gerar descrições.
# MAGIC
# MAGIC A tabela de saída `resultados_metadados.tabela_metadados` usa um **modelo de
# MAGIC entidade**: cada linha representa uma entidade do catálogo (`schema`, `tabela` ou
# MAGIC `coluna`), evitando repetir a descrição da tabela em cada coluna. Cada linha tem:
# MAGIC - `entidade`: tipo da entidade (schema | tabela | coluna)
# MAGIC - `descricao_entidade`: descrição gerada para aquela entidade
# MAGIC - `tags`: tags da entidade obtidas do Unity Catalog (information_schema)
# MAGIC
# MAGIC A tabela será vetorizada posteriormente (Notebook 3) para busca em um RAG.

# COMMAND ----------

# DESCRIÇÃO DA CÉLULA:
# Define os parâmetros de entrada. O nome do catálogo é recebido via widget e os schemas
# de origem (demo_energia) e destino (resultados_metadados) e o endpoint do LLM são
# definidos aqui.
dbutils.widgets.text("catalog", "", "Nome do catálogo")
catalog = dbutils.widgets.get("catalog").strip() if dbutils.widgets.get("catalog") else ""

schema_origem = "demo_energia"
schema_destino = "resultados_metadados"
tabela_destino = "tabela_metadados"
LLM_ENDPOINT = "databricks-meta-llama-3-1-8b-instruct"

print(f"Catálogo: {catalog}")
print(f"Origem: {catalog}.{schema_origem}")
print(f"Destino: {catalog}.{schema_destino}.{tabela_destino}")

# COMMAND ----------

# DESCRIÇÃO DA CÉLULA:
# Cria o schema de destino (resultados_metadados) caso ainda não exista. É nele que a
# tabela final de metadados será gravada.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_destino}")

# COMMAND ----------

# DESCRIÇÃO DA CÉLULA:
# Inicializa o cliente do SDK Databricks e define as funções de LLM: 'chamar_llm' envia
# o prompt para o endpoint Llama 3.1 8B e 'extrair_json' faz o parsing resiliente da
# resposta (que deve vir em JSON).
import json
import re
import hashlib
from datetime import datetime
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

def chamar_llm(prompt: str, max_tokens: int = 1024, temperature: float = 0.2) -> str:
    """Chama o endpoint de chat (Llama 3.1 8B) e retorna o texto da resposta."""
    from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
    resp = w.serving_endpoints.query(
        name=LLM_ENDPOINT,
        messages=[
            ChatMessage(
                role=ChatMessageRole.SYSTEM,
                content=(
                    "Você é um especialista em governança de dados do setor de energia. "
                    "Gere descrições objetivas, em português, para schemas, tabelas e colunas. "
                    "Responda SEMPRE e SOMENTE em JSON válido, sem texto adicional."
                ),
            ),
            ChatMessage(role=ChatMessageRole.USER, content=prompt),
        ],
        max_tokens=max_tokens,
        temperature=temperature,
    )
    return resp.choices[0].message.content

def extrair_json(texto: str) -> dict:
    """Extrai o primeiro objeto JSON de uma resposta de LLM de forma resiliente."""
    if not texto:
        return {}
    texto = texto.strip()
    texto = re.sub(r"^```(json)?", "", texto).strip()
    texto = re.sub(r"```$", "", texto).strip()
    try:
        return json.loads(texto)
    except Exception:
        m = re.search(r"\{.*\}", texto, re.DOTALL)
        if m:
            try:
                return json.loads(m.group(0))
            except Exception:
                return {}
    return {}

def make_id(*partes) -> str:
    """Gera um id determinístico (sha256) a partir das partes informadas."""
    return hashlib.sha256("||".join(p for p in partes if p).encode()).hexdigest()

# COMMAND ----------

# DESCRIÇÃO DA CÉLULA:
# Define as funções que obtêm as TAGS do Unity Catalog via information_schema (a API de
# metadados do UC): tags de schema, de tabela e de colunas. Cada tag é formatada como
# 'nome=valor' (ou apenas 'nome' quando não houver valor).
def _fmt_tag(nome, valor):
    return f"{nome}={valor}" if valor else nome

def get_schema_tags(cat, sch):
    try:
        rows = spark.sql(
            f"SELECT tag_name, tag_value FROM {cat}.information_schema.schema_tags "
            f"WHERE catalog_name='{cat}' AND schema_name='{sch}'"
        ).collect()
        return [_fmt_tag(r.tag_name, r.tag_value) for r in rows]
    except Exception as e:
        print(f"[aviso] schema_tags {cat}.{sch}: {e}")
        return []

def get_table_tags(cat, sch, tbl):
    try:
        rows = spark.sql(
            f"SELECT tag_name, tag_value FROM {cat}.information_schema.table_tags "
            f"WHERE catalog_name='{cat}' AND schema_name='{sch}' AND table_name='{tbl}'"
        ).collect()
        return [_fmt_tag(r.tag_name, r.tag_value) for r in rows]
    except Exception as e:
        print(f"[aviso] table_tags {cat}.{sch}.{tbl}: {e}")
        return []

def get_column_tags(cat, sch, tbl):
    """Retorna {coluna: [tags]} para todas as colunas da tabela."""
    out = {}
    try:
        rows = spark.sql(
            f"SELECT column_name, tag_name, tag_value FROM {cat}.information_schema.column_tags "
            f"WHERE catalog_name='{cat}' AND schema_name='{sch}' AND table_name='{tbl}'"
        ).collect()
        for r in rows:
            out.setdefault(r.column_name, []).append(_fmt_tag(r.tag_name, r.tag_value))
    except Exception as e:
        print(f"[aviso] column_tags {cat}.{sch}.{tbl}: {e}")
    return out

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lista de tabelas do schema de origem

# COMMAND ----------

# DESCRIÇÃO DA CÉLULA:
# Lista todas as tabelas do schema de origem (demo_energia) consultando o Unity Catalog
# via SDK (w.tables.list).
tabelas = [t.name for t in w.tables.list(catalog_name=catalog, schema_name=schema_origem)]
print(f"{len(tabelas)} tabelas encontradas:", tabelas)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Geração das entidades (tabela + colunas)

# COMMAND ----------

# DESCRIÇÃO DA CÉLULA:
# Laço principal. Para cada tabela: obtém os metadados do Unity Catalog via SDK
# (w.tables.get) — nome/tipo/comentário de cada coluna — e as tags (de tabela e de
# coluna) via information_schema. Lê 100 linhas de amostra, chama o LLM para gerar a
# descrição da tabela e de cada coluna e cria UMA entidade 'tabela' e N entidades
# 'coluna' (sem repetir a descrição da tabela nas colunas).
entidades = []
descricoes_tabelas = {}  # usado depois para descrever o schema

for tabela in tabelas:
    full_name = f"{catalog}.{schema_origem}.{tabela}"
    print(f"\n=== Processando {full_name} ===")

    # Metadados direto do Unity Catalog (SDK)
    tabela_uc = w.tables.get(full_name)
    colunas_uc = tabela_uc.columns or []
    colunas = [c.name for c in colunas_uc]
    dtypes = {c.name: (c.type_text or "") for c in colunas_uc}
    comentarios_uc = {c.name: (c.comment or "") for c in colunas_uc}
    comentario_tabela_uc = tabela_uc.comment or ""

    # Tags do Unity Catalog (information_schema)
    table_tags = get_table_tags(catalog, schema_origem, tabela)
    col_tags_map = get_column_tags(catalog, schema_origem, tabela)

    # Amostra das primeiras 100 linhas (contexto de valores para o LLM)
    df = spark.table(full_name)
    amostra = df.limit(100).toPandas()
    amostra_str = amostra.head(20).to_csv(index=False)

    colunas_contexto = {
        c: {"tipo": dtypes.get(c, ""), "comentario_existente": comentarios_uc.get(c, "")}
        for c in colunas
    }

    prompt = f"""
Analise a tabela '{tabela}' de um catálogo de dados do setor de energia elétrica.

Comentário atual da tabela no Unity Catalog: {comentario_tabela_uc or "(sem comentário)"}

Colunas (nome, tipo e comentário existente no Unity Catalog):
{json.dumps(colunas_contexto, ensure_ascii=False, indent=2)}

Amostra de dados (CSV, até 20 linhas das 100 lidas):
{amostra_str}

Gere um JSON com EXATAMENTE esta estrutura:
{{
  "descricao_tabela": "descrição clara do que a tabela representa (1-3 frases)",
  "colunas": {{
    "<nome_coluna>": {{"descricao": "..."}}
  }}
}}
Inclua TODAS as colunas: {colunas}.
Responda apenas com o JSON.
"""

    resposta = chamar_llm(prompt, max_tokens=2048)
    parsed = extrair_json(resposta)

    descricao_tabela = parsed.get("descricao_tabela", "")
    colunas_meta = parsed.get("colunas", {}) or {}
    descricoes_tabelas[tabela] = descricao_tabela

    # 1 entidade 'tabela'
    entidades.append({
        "id": make_id(full_name, "tabela"),
        "entidade": "tabela",
        "catalogo": catalog,
        "schema": schema_origem,
        "tabela": tabela,
        "coluna": None,
        "nome_completo": full_name,
        "tipo_coluna": None,
        "descricao_entidade": descricao_tabela,
        "tags": table_tags,
        "gerado_em": datetime.utcnow().isoformat(),
    })

    # N entidades 'coluna'
    for col in colunas:
        cmeta = colunas_meta.get(col, {}) or {}
        entidades.append({
            "id": make_id(full_name, col, "coluna"),
            "entidade": "coluna",
            "catalogo": catalog,
            "schema": schema_origem,
            "tabela": tabela,
            "coluna": col,
            "nome_completo": f"{full_name}.{col}",
            "tipo_coluna": dtypes.get(col, ""),
            "descricao_entidade": cmeta.get("descricao", "") or comentarios_uc.get(col, ""),
            "tags": col_tags_map.get(col, []),
            "gerado_em": datetime.utcnow().isoformat(),
        })

    print(f"  -> tabela + {len(colunas)} colunas")

print(f"\nEntidades de tabela/coluna geradas: {len(entidades)}")

# COMMAND ----------

# DESCRIÇÃO DA CÉLULA:
# Gera a entidade 'schema': cria uma descrição do schema demo_energia a partir das
# descrições das tabelas (via LLM) e obtém as tags do schema no Unity Catalog. Adiciona
# essa única linha à lista de entidades.
resumo_tabelas = "; ".join(f"{t}: {d}" for t, d in descricoes_tabelas.items() if d)
prompt_schema = f"""
Resuma em 1-2 frases, em português, o propósito do schema '{schema_origem}' de um
catálogo do setor de energia, que contém as seguintes tabelas e descrições:
{resumo_tabelas}

Responda em JSON: {{"descricao_schema": "..."}}
"""
parsed_schema = extrair_json(chamar_llm(prompt_schema, max_tokens=512))
descricao_schema = parsed_schema.get("descricao_schema", "") or f"Schema {schema_origem} do setor de energia."

schema_full = f"{catalog}.{schema_origem}"
entidades.append({
    "id": make_id(schema_full, "schema"),
    "entidade": "schema",
    "catalogo": catalog,
    "schema": schema_origem,
    "tabela": None,
    "coluna": None,
    "nome_completo": schema_full,
    "tipo_coluna": None,
    "descricao_entidade": descricao_schema,
    "tags": get_schema_tags(catalog, schema_origem),
    "gerado_em": datetime.utcnow().isoformat(),
})
print(f"Total de entidades (schema + tabelas + colunas): {len(entidades)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aplicação das descrições como COMMENT no Unity Catalog
# MAGIC
# MAGIC As descrições geradas pelo LLM são gravadas de volta como comentários nas tabelas,
# MAGIC colunas e no schema de origem (`demo_energia`), populando a documentação no UC.

# COMMAND ----------

# DESCRIÇÃO DA CÉLULA:
# Função auxiliar que escapa aspas simples para uso seguro em comandos SQL de COMMENT.
def esc(texto: str) -> str:
    return (texto or "").replace("'", "''")

# COMMAND ----------

# DBTITLE 1,Cell 13
# DESCRIÇÃO DA CÉLULA:
# Percorre as entidades geradas e aplica as descrições como COMMENT no Unity Catalog:
# - entidade 'schema' -> COMMENT ON SCHEMA
# - entidade 'tabela' -> COMMENT ON TABLE
# - entidade 'coluna' -> ALTER TABLE ... ALTER COLUMN ... COMMENT
# Comentários vazios são ignorados; erros por entidade são reportados sem interromper.
aplicados = {"schema": 0, "tabela": 0, "coluna": 0}

for ent in entidades:
    descricao = ent.get("descricao_entidade", "")
    if not descricao:
        continue
    tipo = ent["entidade"]
    try:
        if tipo == "schema":
            spark.sql(f"COMMENT ON SCHEMA {ent['nome_completo']} IS '{esc(descricao)}'")
            aplicados["schema"] += 1
        elif tipo == "tabela":
            spark.sql(f"COMMENT ON TABLE {ent['nome_completo']} IS '{esc(descricao)}'")
            aplicados["tabela"] += 1
        elif tipo == "coluna":
            tabela_full = f"{ent['catalogo']}.{ent['schema']}.{ent['tabela']}"
            spark.sql(f"ALTER TABLE {tabela_full} ALTER COLUMN `{ent['coluna']}` COMMENT '{esc(descricao)}'")
            aplicados["coluna"] += 1
    except Exception as e:
        print(f"[aviso] COMMENT {tipo} {ent.get('nome_completo')}: {e}")

print("Comentários aplicados no Unity Catalog:", aplicados)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persistência em resultados_metadados.tabela_metadados
# MAGIC
# MAGIC Modelo de entidade (uma linha por schema/tabela/coluna). Inclui `texto_busca`
# MAGIC consolidado e habilita Change Data Feed para o Delta Sync Index (Notebook 3).

# COMMAND ----------

# DESCRIÇÃO DA CÉLULA:
# Converte a lista de entidades em um DataFrame Spark com schema explícito e cria a
# coluna 'texto_busca' (concatenando entidade, nomes, tipo, descrição e tags) que será
# usada para o embedding/busca semântica.
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

schema_spark = StructType([
    StructField("id", StringType()),
    StructField("entidade", StringType()),
    StructField("catalogo", StringType()),
    StructField("schema", StringType()),
    StructField("tabela", StringType()),
    StructField("coluna", StringType()),
    StructField("nome_completo", StringType()),
    StructField("tipo_coluna", StringType()),
    StructField("descricao_entidade", StringType()),
    StructField("tags", ArrayType(StringType())),
    StructField("gerado_em", StringType()),
])

df_meta = spark.createDataFrame(entidades, schema=schema_spark)

df_meta = df_meta.withColumn(
    "texto_busca",
    F.concat_ws(
        " | ",
        F.concat(F.lit("Entidade: "), F.col("entidade")),
        F.concat(F.lit("Nome: "), F.col("nome_completo")),
        F.when(F.col("tipo_coluna").isNotNull(), F.concat(F.lit("Tipo: "), F.col("tipo_coluna"))).otherwise(F.lit("")),
        F.concat(F.lit("Descrição: "), F.coalesce(F.col("descricao_entidade"), F.lit(""))),
        F.concat(F.lit("Tags: "), F.concat_ws(", ", F.col("tags"))),
    ),
)

# COMMAND ----------

# DESCRIÇÃO DA CÉLULA:
# Grava o DataFrame de entidades na tabela final (resultados_metadados.tabela_metadados),
# habilita o Change Data Feed (necessário para o Delta Sync Index do Vector Search no
# Notebook 3) e exibe o resultado.
destino_full = f"{catalog}.{schema_destino}.{tabela_destino}"
(
    df_meta.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .option("delta.enableChangeDataFeed", "true")
    .saveAsTable(destino_full)
)

spark.sql(f"ALTER TABLE {destino_full} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

print(f"Gravado: {destino_full}")
display(spark.table(destino_full))