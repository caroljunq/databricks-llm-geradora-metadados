# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 3 - Criador do Vector Search + Sync
# MAGIC
# MAGIC Cria um endpoint de Vector Search e um Delta Sync Index que sincroniza e gera
# MAGIC embeddings da tabela `resultados_metadados.tabela_metadados`, permitindo busca
# MAGIC semântica (RAG) sobre os metadados de tabelas e colunas.

# COMMAND ----------

# MAGIC %pip install --quiet databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Nome do catálogo")
dbutils.widgets.text("vs_endpoint", "vs_metadados_endpoint", "Vector Search Endpoint")
dbutils.widgets.text("embedding_model", "databricks-gte-large-en", "Modelo de embedding")

catalog = dbutils.widgets.get("catalog").strip()
vs_endpoint = dbutils.widgets.get("vs_endpoint").strip()
embedding_model = dbutils.widgets.get("embedding_model").strip()

schema_destino = "resultados_metadados"
source_table = f"{catalog}.{schema_destino}.tabela_metadados"
index_name = f"{catalog}.{schema_destino}.tabela_metadados_index"

print(f"Source table : {source_table}")
print(f"Index        : {index_name}")
print(f"Endpoint     : {vs_endpoint}")
print(f"Embedding    : {embedding_model}")

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient(disable_notice=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Endpoint de Vector Search

# COMMAND ----------

existing = [e["name"] for e in vsc.list_endpoints().get("endpoints", [])]
if vs_endpoint not in existing:
    print(f"Criando endpoint {vs_endpoint} ...")
    vsc.create_endpoint(name=vs_endpoint, endpoint_type="STANDARD")
else:
    print(f"Endpoint {vs_endpoint} já existe.")

vsc.wait_for_endpoint(name=vs_endpoint, verbose=True)
print("Endpoint pronto.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Garante Change Data Feed na tabela de origem

# COMMAND ----------

spark.sql(f"ALTER TABLE {source_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
print("CDF habilitado em", source_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Delta Sync Index (embeddings gerenciados pelo Databricks)
# MAGIC
# MAGIC O índice embeda a coluna `texto_busca`, que consolida nome/descrição/tags de
# MAGIC tabela e coluna. A sincronização é contínua (`CONTINUOUS`).

# COMMAND ----------

def index_existe(idx_name: str) -> bool:
    try:
        idxs = vsc.list_indexes(name=vs_endpoint).get("vector_indexes", [])
        return any(i["name"] == idx_name for i in idxs)
    except Exception:
        return False

if not index_existe(index_name):
    print(f"Criando index {index_name} ...")
    vsc.create_delta_sync_index(
        endpoint_name=vs_endpoint,
        index_name=index_name,
        source_table_name=source_table,
        pipeline_type="TRIGGERED",
        primary_key="id",
        embedding_source_column="texto_busca",
        embedding_model_endpoint_name=embedding_model,
    )
else:
    print(f"Index {index_name} já existe.")

index = vsc.get_index(endpoint_name=vs_endpoint, index_name=index_name)
print("Index obtido.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Dispara sincronização e aguarda ficar online

# COMMAND ----------

try:
    index.sync()
    print("Sync disparado.")
except Exception as e:
    print(f"[aviso] sync: {e}")

import time
for _ in range(60):
    try:
        status = index.describe().get("status", {})
        ready = status.get("ready", False)
        detail = status.get("detailed_state", "")
        print(f"ready={ready} state={detail}")
        if ready:
            break
    except Exception as e:
        print(f"[aviso] describe: {e}")
    time.sleep(20)

# COMMAND ----------

# DBTITLE 1,Diagnóstico do endpoint
index = vsc.get_index(endpoint_name=vs_endpoint, index_name=index_name)
status = index.describe().get("status", {})
print("Ready  :", status.get("ready"))
print("State  :", status.get("detailed_state"))
print("Message:", status.get("message", ""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Teste rápido de busca

# COMMAND ----------

try:
    results = index.similarity_search(
        query_text="consumo de energia por cliente",
        columns=["id", "entidade", "nome_completo", "tabela", "coluna", "tipo_coluna", "descricao_entidade", "tags"],
        num_results=5,
    )
    display(results)
except Exception as e:
    print(f"[aviso] busca de teste (índice pode ainda estar indexando): {e}")

# COMMAND ----------

print("Pronto. Use estes valores no app Streamlit:")
print(f"  VECTOR_SEARCH_ENDPOINT = {vs_endpoint}")
print(f"  VECTOR_SEARCH_INDEX    = {index_name}")