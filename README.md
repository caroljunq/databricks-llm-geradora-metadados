# databricks-llm-geradora-metadados

Databricks App + notebooks para **pesquisar e gerar metadados de tabelas do Unity Catalog** usando LLM e Vector Search.

## Objetivo

O objetivo deste projeto é criar um **Databricks App** para **pesquisar e gerar metadados de tabelas do Unity Catalog**.

O fluxo end-to-end é:

1. **Gera dados sintéticos** de um domínio realista (setor de energia elétrica) no Unity Catalog.
2. **Gera metadados com LLM** — usa o endpoint `databricks-meta-llama-3-1-8b-instruct` para produzir descrições de schemas, tabelas e colunas, gravando-as de volta como `COMMENT` no Unity Catalog e em uma tabela pesquisável.
3. **Indexa para busca semântica** — cria um endpoint e um índice de Vector Search (RAG) sobre os metadados gerados.
4. **Disponibiliza um app Streamlit** onde o usuário pesquisa, por linguagem natural, schemas/tabelas/colunas e suas tags.

## Dataset gerado (schema `demo_energia`)

O Notebook 1 popula o schema `demo_energia` com **10 tabelas** de dados sintéticos coerentes com o setor de energia elétrica (distribuidora de energia). Cada tabela e coluna recebe documentação (`COMMENT`) no Unity Catalog.

| # | Tabela | Linhas (aprox.) | Descrição |
|---|--------|-----------------|-----------|
| 1 | `clientes` | 2.000 | Cadastro dos clientes da distribuidora, com identificação, tipo de cliente, segmento de tensão e localização (cidade/UF). |
| 2 | `instalacoes` | 2.500 | Unidades consumidoras (instalações elétricas) vinculadas aos clientes, com características técnicas (tipo de ligação, tensão nominal) e geolocalização. |
| 3 | `equipamentos` | 1.500 | Equipamentos da rede elétrica (transformadores, disjuntores, religadores, etc.) com fabricante, potência, status e subestação. |
| 4 | `medidores` | 2.500 | Medidores de energia instalados nas unidades consumidoras, com número de série, tecnologia e datas de instalação/aferição. |
| 5 | `leituras_medidores` | 50.000 | Registros de leitura dos medidores ao longo do tempo, base para o cálculo de consumo (leitura acumulada, consumo apurado, origem). |
| 6 | `consumo_energia` | 60.000 | Consumo mensal de energia por instalação, com demanda máxima, fator de potência e separação por horário tarifário (ponta / fora de ponta). |
| 7 | `tarifas` | 12 | Tabela de tarifas de energia por classe de consumo e modalidade tarifária (Convencional, Branca, Verde, Azul). |
| 8 | `faturas` | 40.000 | Faturas mensais emitidas para os clientes, com consumo, valor total e status de pagamento (Paga, Em Aberto, Vencida). |
| 9 | `manutencao_equipamentos` | 4.000 | Histórico de manutenções (preventiva, corretiva, preditiva) nos equipamentos da rede, com custo, duração e técnico responsável. |
| 10 | `interrupcoes` | 800 | Eventos de interrupção no fornecimento de energia, com tipo, causa raiz, duração e quantidade de clientes afetados. |

Os dados são gerados de forma **determinística** (funções `rand(seed)` e `spark.range`), garantindo reprodutibilidade. As tabelas são relacionadas entre si por chaves como `cliente_id`, `instalacao_id`, `equipamento_id`, `medidor_id` e `tarifa_id`.

### Tabela de metadados (schema `resultados_metadados`)

O Notebook 2 grava os metadados gerados pelo LLM em `resultados_metadados.tabela_metadados`, usando um **modelo de entidade** (uma linha por `schema`, `tabela` ou `coluna`). Colunas principais:

- `id` — identificador determinístico (sha256) da entidade.
- `entidade` — tipo: `schema`, `tabela` ou `coluna`.
- `catalogo`, `schema`, `tabela`, `coluna`, `nome_completo` — localização da entidade no Unity Catalog.
- `tipo_coluna` — tipo de dado (apenas para entidades `coluna`).
- `descricao_entidade` — descrição gerada pelo LLM.
- `tags` — tags da entidade obtidas do `information_schema` do Unity Catalog.
- `texto_busca` — texto consolidado (nome + descrição + tipo + tags) usado para gerar os embeddings.
- `gerado_em` — timestamp de geração.

Essa tabela tem o **Change Data Feed** habilitado, requisito para o Delta Sync Index do Vector Search.

## Estrutura do projeto

```
databricks-llm-geradora-metadados/
├── README.md
├── LICENSE
├── app/                        # Databricks App (Streamlit) de busca de metadados
│   ├── app.py                  # Frontend de pesquisa semântica
│   ├── app.yaml                # Configuração do app (comando + variáveis de ambiente)
│   └── requirements.txt        # Dependências do app
└── notebooks/                  # Pipeline de geração e indexação dos metadados
    ├── 01_gerador_de_dados.py
    ├── 02_job_gerador_de_metadados.py
    └── 03_vectorsearch_sync.py
```

### Arquivos

**`notebooks/01_gerador_de_dados.py`** — Cria o catálogo (recebido via widget `catalog`) e o schema `demo_energia`, gera as 10 tabelas de dados sintéticos do setor de energia e aplica `COMMENT` de documentação em cada tabela e coluna.

**`notebooks/02_job_gerador_de_metadados.py`** — Lê os metadados do schema `demo_energia` diretamente do Unity Catalog (via SDK) e suas tags (via `information_schema`). Para cada tabela, envia uma amostra de dados e o contexto das colunas ao LLM `databricks-meta-llama-3-1-8b-instruct` para gerar descrições. As descrições são gravadas de volta como `COMMENT` no Unity Catalog e persistidas na tabela `resultados_metadados.tabela_metadados` (modelo de entidade, com `texto_busca` e Change Data Feed habilitado).

**`notebooks/03_vectorsearch_sync.py`** — Cria um endpoint de Vector Search e um **Delta Sync Index** que sincroniza e gera embeddings da coluna `texto_busca` da tabela de metadados (modelo de embedding `databricks-gte-large-en`), habilitando a busca semântica (RAG). Ao final imprime os valores (`VECTOR_SEARCH_ENDPOINT` e `VECTOR_SEARCH_INDEX`) a serem usados no app.

**`app/app.py`** — App Streamlit "Buscador de Metadados". Faz pesquisa semântica de schemas, tabelas e colunas (incluindo tags) no índice de Vector Search e lê detalhes das colunas diretamente do Unity Catalog via SDK. Permite filtrar por tipo de entidade e ajustar o número de resultados.

**`app/app.yaml`** — Configuração do Databricks App: comando de inicialização do Streamlit (porta 8000) e variáveis de ambiente (`VECTOR_SEARCH_ENDPOINT`, `VECTOR_SEARCH_INDEX`, `METADATA_TABLE`).

**`app/requirements.txt`** — Dependências do app: `streamlit`, `databricks-sdk` e `databricks-vectorsearch`.

## Como usar

1. **Notebook 1** — Execute informando o widget `catalog` para gerar o schema `demo_energia` e as 10 tabelas.
2. **Notebook 2** — Execute (mesmo `catalog`) para gerar os metadados com o LLM e gravar `resultados_metadados.tabela_metadados`.
3. **Notebook 3** — Execute para criar o endpoint e o índice de Vector Search. Anote os valores impressos ao final.
4. **App** — Atualize o `app/app.yaml` (ou as variáveis de ambiente) com o endpoint e o índice gerados e faça o deploy do Databricks App.
