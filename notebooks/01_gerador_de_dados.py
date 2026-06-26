# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 1 - Gerador de Dados
# MAGIC
# MAGIC Popula um catálogo (recebido via widget) com o schema `demo_energia` contendo
# MAGIC 10 tabelas relacionadas ao ramo de energia, incluindo documentação (COMMENT)
# MAGIC de cada tabela e de cada coluna.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Nome do catálogo")
catalog = dbutils.widgets.get("catalog").strip()

schema = "demo_energia"
print(f"Catálogo: {catalog} | Schema: {schema}")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")
print("Catálogo e schema prontos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Geração das 10 tabelas
# MAGIC
# MAGIC Usamos funções Spark (`range` + funções pseudo-aleatórias determinísticas) para
# MAGIC gerar dados sintéticos coerentes com o setor de energia.

# COMMAND ----------

from pyspark.sql import functions as F

N_CLIENTES = 2000
N_INSTALACOES = 2500
N_EQUIPAMENTOS = 1500
N_MEDIDORES = 2500
N_LEITURAS = 50000
N_CONSUMO = 60000
N_FATURAS = 40000
N_MANUTENCOES = 4000
N_INTERRUPCOES = 800
N_TARIFAS = 12

# COMMAND ----------

# 1) clientes
clientes = (
    spark.range(1, N_CLIENTES + 1)
    .withColumnRenamed("id", "cliente_id")
    .withColumn("nome", F.concat(F.lit("Cliente "), F.col("cliente_id").cast("string")))
    .withColumn("tipo_cliente", F.element_at(F.array(F.lit("Residencial"), F.lit("Comercial"), F.lit("Industrial"), F.lit("Rural")), (F.col("cliente_id") % 4 + 1).cast("int")))
    .withColumn("segmento", F.element_at(F.array(F.lit("Baixa Tensão"), F.lit("Média Tensão"), F.lit("Alta Tensão")), (F.col("cliente_id") % 3 + 1).cast("int")))
    .withColumn("cidade", F.element_at(F.array(F.lit("São Paulo"), F.lit("Rio de Janeiro"), F.lit("Belo Horizonte"), F.lit("Curitiba"), F.lit("Recife")), (F.col("cliente_id") % 5 + 1).cast("int")))
    .withColumn("uf", F.element_at(F.array(F.lit("SP"), F.lit("RJ"), F.lit("MG"), F.lit("PR"), F.lit("PE")), (F.col("cliente_id") % 5 + 1).cast("int")))
    .withColumn("data_cadastro", F.expr("date_sub(current_date(), cast(rand(1)*2000 as int))"))
    .withColumn("ativo", (F.col("cliente_id") % 10 != 0))
)
clientes.write.mode("overwrite").saveAsTable("clientes")

# COMMAND ----------

# 2) instalacoes
instalacoes = (
    spark.range(1, N_INSTALACOES + 1)
    .withColumnRenamed("id", "instalacao_id")
    .withColumn("cliente_id", (F.col("instalacao_id") % N_CLIENTES + 1))
    .withColumn("endereco", F.concat(F.lit("Rua "), (F.col("instalacao_id") % 500 + 1).cast("string"), F.lit(", nº "), (F.col("instalacao_id") % 999 + 1).cast("string")))
    .withColumn("tipo_ligacao", F.element_at(F.array(F.lit("Monofásica"), F.lit("Bifásica"), F.lit("Trifásica")), (F.col("instalacao_id") % 3 + 1).cast("int")))
    .withColumn("tensao_nominal_v", F.element_at(F.array(F.lit(127), F.lit(220), F.lit(380)), (F.col("instalacao_id") % 3 + 1).cast("int")))
    .withColumn("latitude", F.round(F.lit(-23.5) + F.rand(2) * 5 - 2.5, 6))
    .withColumn("longitude", F.round(F.lit(-46.6) + F.rand(3) * 5 - 2.5, 6))
    .withColumn("data_ativacao", F.expr("date_sub(current_date(), cast(rand(4)*1800 as int))"))
)
instalacoes.write.mode("overwrite").saveAsTable("instalacoes")

# COMMAND ----------

# 3) equipamentos
equipamentos = (
    spark.range(1, N_EQUIPAMENTOS + 1)
    .withColumnRenamed("id", "equipamento_id")
    .withColumn("tipo_equipamento", F.element_at(F.array(F.lit("Transformador"), F.lit("Disjuntor"), F.lit("Religador"), F.lit("Capacitor"), F.lit("Chave Seccionadora")), (F.col("equipamento_id") % 5 + 1).cast("int")))
    .withColumn("fabricante", F.element_at(F.array(F.lit("WEG"), F.lit("Siemens"), F.lit("ABB"), F.lit("Schneider")), (F.col("equipamento_id") % 4 + 1).cast("int")))
    .withColumn("potencia_kva", F.round(F.rand(5) * 1000 + 50, 1))
    .withColumn("data_instalacao", F.expr("date_sub(current_date(), cast(rand(6)*3000 as int))"))
    .withColumn("status", F.element_at(F.array(F.lit("Operacional"), F.lit("Em Manutenção"), F.lit("Desativado")), (F.col("equipamento_id") % 3 + 1).cast("int")))
    .withColumn("subestacao", F.concat(F.lit("SE-"), (F.col("equipamento_id") % 30 + 1).cast("string")))
)
equipamentos.write.mode("overwrite").saveAsTable("equipamentos")

# COMMAND ----------

# 4) medidores
medidores = (
    spark.range(1, N_MEDIDORES + 1)
    .withColumnRenamed("id", "medidor_id")
    .withColumn("instalacao_id", (F.col("medidor_id") % N_INSTALACOES + 1))
    .withColumn("numero_serie", F.concat(F.lit("MD"), F.lpad(F.col("medidor_id").cast("string"), 8, "0")))
    .withColumn("tipo_medidor", F.element_at(F.array(F.lit("Eletromecânico"), F.lit("Eletrônico"), F.lit("Inteligente (Smart)")), (F.col("medidor_id") % 3 + 1).cast("int")))
    .withColumn("data_instalacao", F.expr("date_sub(current_date(), cast(rand(7)*2500 as int))"))
    .withColumn("ultima_aferição", F.expr("date_sub(current_date(), cast(rand(8)*365 as int))"))
)
medidores.write.mode("overwrite").saveAsTable("medidores")

# COMMAND ----------

# 5) leituras_medidores
leituras = (
    spark.range(1, N_LEITURAS + 1)
    .withColumnRenamed("id", "leitura_id")
    .withColumn("medidor_id", (F.col("leitura_id") % N_MEDIDORES + 1))
    .withColumn("data_leitura", F.expr("date_sub(current_date(), cast(rand(9)*365 as int))"))
    .withColumn("leitura_kwh", F.round(F.rand(10) * 5000 + 100, 2))
    .withColumn("consumo_kwh", F.round(F.rand(11) * 800 + 10, 2))
    .withColumn("origem_leitura", F.element_at(F.array(F.lit("Presencial"), F.lit("Telemetria"), F.lit("Autoleitura")), (F.col("leitura_id") % 3 + 1).cast("int")))
)
leituras.write.mode("overwrite").saveAsTable("leituras_medidores")

# COMMAND ----------

# 6) consumo_energia
consumo = (
    spark.range(1, N_CONSUMO + 1)
    .withColumnRenamed("id", "consumo_id")
    .withColumn("instalacao_id", (F.col("consumo_id") % N_INSTALACOES + 1))
    .withColumn("mes_referencia", F.expr("trunc(date_sub(current_date(), cast(rand(12)*720 as int)), 'MM')"))
    .withColumn("consumo_kwh", F.round(F.rand(13) * 1200 + 50, 2))
    .withColumn("demanda_maxima_kw", F.round(F.rand(14) * 100 + 5, 2))
    .withColumn("fator_potencia", F.round(F.rand(15) * 0.2 + 0.8, 3))
    .withColumn("horario_ponta_kwh", F.round(F.rand(16) * 300, 2))
    .withColumn("horario_fora_ponta_kwh", F.round(F.rand(17) * 900, 2))
)
consumo.write.mode("overwrite").saveAsTable("consumo_energia")

# COMMAND ----------

# 7) tarifas
tarifas = (
    spark.range(1, N_TARIFAS + 1)
    .withColumnRenamed("id", "tarifa_id")
    .withColumn("nome_tarifa", F.element_at(F.array(F.lit("Convencional"), F.lit("Branca"), F.lit("Verde"), F.lit("Azul")), (F.col("tarifa_id") % 4 + 1).cast("int")))
    .withColumn("classe_consumo", F.element_at(F.array(F.lit("Residencial"), F.lit("Comercial"), F.lit("Industrial"), F.lit("Rural")), (F.col("tarifa_id") % 4 + 1).cast("int")))
    .withColumn("valor_kwh_reais", F.round(F.rand(18) * 0.8 + 0.4, 4))
    .withColumn("valor_demanda_reais", F.round(F.rand(19) * 30 + 10, 2))
    .withColumn("vigencia_inicio", F.expr("date_sub(current_date(), cast(rand(20)*400 as int))"))
)
tarifas.write.mode("overwrite").saveAsTable("tarifas")

# COMMAND ----------

# 8) faturas
faturas = (
    spark.range(1, N_FATURAS + 1)
    .withColumnRenamed("id", "fatura_id")
    .withColumn("cliente_id", (F.col("fatura_id") % N_CLIENTES + 1))
    .withColumn("instalacao_id", (F.col("fatura_id") % N_INSTALACOES + 1))
    .withColumn("tarifa_id", (F.col("fatura_id") % N_TARIFAS + 1))
    .withColumn("mes_referencia", F.expr("trunc(date_sub(current_date(), cast(rand(21)*720 as int)), 'MM')"))
    .withColumn("consumo_kwh", F.round(F.rand(22) * 1000 + 50, 2))
    .withColumn("valor_total_reais", F.round(F.rand(23) * 800 + 50, 2))
    .withColumn("status_pagamento", F.element_at(F.array(F.lit("Paga"), F.lit("Em Aberto"), F.lit("Vencida")), (F.col("fatura_id") % 3 + 1).cast("int")))
    .withColumn("data_vencimento", F.expr("date_add(current_date(), cast(rand(24)*30 as int) - 15)"))
)
faturas.write.mode("overwrite").saveAsTable("faturas")

# COMMAND ----------

# 9) manutencao_equipamentos
manutencoes = (
    spark.range(1, N_MANUTENCOES + 1)
    .withColumnRenamed("id", "manutencao_id")
    .withColumn("equipamento_id", (F.col("manutencao_id") % N_EQUIPAMENTOS + 1))
    .withColumn("tipo_manutencao", F.element_at(F.array(F.lit("Preventiva"), F.lit("Corretiva"), F.lit("Preditiva")), (F.col("manutencao_id") % 3 + 1).cast("int")))
    .withColumn("data_manutencao", F.expr("date_sub(current_date(), cast(rand(25)*1000 as int))"))
    .withColumn("custo_reais", F.round(F.rand(26) * 5000 + 200, 2))
    .withColumn("duracao_horas", F.round(F.rand(27) * 12 + 1, 1))
    .withColumn("tecnico_responsavel", F.concat(F.lit("Técnico "), (F.col("manutencao_id") % 50 + 1).cast("string")))
    .withColumn("resultado", F.element_at(F.array(F.lit("Concluída"), F.lit("Pendente"), F.lit("Reagendada")), (F.col("manutencao_id") % 3 + 1).cast("int")))
)
manutencoes.write.mode("overwrite").saveAsTable("manutencao_equipamentos")

# COMMAND ----------

# 10) interrupcoes
interrupcoes = (
    spark.range(1, N_INTERRUPCOES + 1)
    .withColumnRenamed("id", "interrupcao_id")
    .withColumn("equipamento_id", (F.col("interrupcao_id") % N_EQUIPAMENTOS + 1))
    .withColumn("instalacao_id", (F.col("interrupcao_id") % N_INSTALACOES + 1))
    .withColumn("tipo_interrupcao", F.element_at(F.array(F.lit("Programada"), F.lit("Não Programada"), F.lit("Emergencial")), (F.col("interrupcao_id") % 3 + 1).cast("int")))
    .withColumn("causa", F.element_at(F.array(F.lit("Tempestade"), F.lit("Sobrecarga"), F.lit("Falha de Equipamento"), F.lit("Manutenção"), F.lit("Vandalismo")), (F.col("interrupcao_id") % 5 + 1).cast("int")))
    .withColumn("data_inicio", F.expr("date_sub(current_timestamp(), cast(rand(28)*365 as int))"))
    .withColumn("duracao_minutos", F.round(F.rand(29) * 480 + 5, 0).cast("int"))
    .withColumn("clientes_afetados", F.round(F.rand(30) * 5000 + 1, 0).cast("int"))
)
interrupcoes.write.mode("overwrite").saveAsTable("interrupcoes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Documentação (COMMENT) das tabelas e colunas

# COMMAND ----------

docs = {
    "clientes": {
        "_table": "Cadastro dos clientes da distribuidora de energia, com dados de identificação, segmento de consumo e localização.",
        "cliente_id": "Identificador único do cliente.",
        "nome": "Nome ou razão social do cliente.",
        "tipo_cliente": "Categoria do cliente: Residencial, Comercial, Industrial ou Rural.",
        "segmento": "Segmento de tensão de fornecimento (Baixa, Média ou Alta Tensão).",
        "cidade": "Município onde o cliente está localizado.",
        "uf": "Unidade federativa (estado) do cliente.",
        "data_cadastro": "Data em que o cliente foi cadastrado na base.",
        "ativo": "Indica se o cliente está ativo (true) ou inativo (false).",
    },
    "instalacoes": {
        "_table": "Unidades consumidoras (instalações elétricas) vinculadas aos clientes, com características técnicas e geolocalização.",
        "instalacao_id": "Identificador único da instalação/unidade consumidora.",
        "cliente_id": "Referência ao cliente proprietário da instalação.",
        "endereco": "Endereço físico da instalação.",
        "tipo_ligacao": "Tipo de ligação elétrica: Monofásica, Bifásica ou Trifásica.",
        "tensao_nominal_v": "Tensão nominal de fornecimento em volts.",
        "latitude": "Latitude geográfica da instalação.",
        "longitude": "Longitude geográfica da instalação.",
        "data_ativacao": "Data de ativação do fornecimento na instalação.",
    },
    "equipamentos": {
        "_table": "Equipamentos da rede elétrica (transformadores, disjuntores, etc.) com dados de fabricante, capacidade e status operacional.",
        "equipamento_id": "Identificador único do equipamento.",
        "tipo_equipamento": "Tipo do equipamento de rede (Transformador, Disjuntor, Religador, etc.).",
        "fabricante": "Fabricante do equipamento.",
        "potencia_kva": "Potência nominal do equipamento em kVA.",
        "data_instalacao": "Data de instalação do equipamento na rede.",
        "status": "Status operacional atual do equipamento.",
        "subestacao": "Subestação à qual o equipamento pertence.",
    },
    "medidores": {
        "_table": "Medidores de energia instalados nas unidades consumidoras, usados para registrar o consumo.",
        "medidor_id": "Identificador único do medidor.",
        "instalacao_id": "Referência à instalação onde o medidor está instalado.",
        "numero_serie": "Número de série único do medidor.",
        "tipo_medidor": "Tecnologia do medidor (Eletromecânico, Eletrônico, Inteligente).",
        "data_instalacao": "Data de instalação do medidor.",
        "ultima_afericao": "Data da última aferição/calibração do medidor.",
    },
    "leituras_medidores": {
        "_table": "Registros de leitura dos medidores ao longo do tempo, base para o cálculo de consumo.",
        "leitura_id": "Identificador único da leitura.",
        "medidor_id": "Referência ao medidor que originou a leitura.",
        "data_leitura": "Data em que a leitura foi realizada.",
        "leitura_kwh": "Valor acumulado registrado no medidor em kWh.",
        "consumo_kwh": "Consumo apurado no período em kWh.",
        "origem_leitura": "Forma de obtenção da leitura (Presencial, Telemetria, Autoleitura).",
    },
    "consumo_energia": {
        "_table": "Consumo mensal de energia por instalação, incluindo demanda e separação por horário tarifário.",
        "consumo_id": "Identificador único do registro de consumo.",
        "instalacao_id": "Referência à instalação consumidora.",
        "mes_referencia": "Mês de referência do consumo.",
        "consumo_kwh": "Consumo total de energia no mês em kWh.",
        "demanda_maxima_kw": "Demanda máxima registrada no mês em kW.",
        "fator_potencia": "Fator de potência médio da instalação no período.",
        "horario_ponta_kwh": "Consumo no horário de ponta em kWh.",
        "horario_fora_ponta_kwh": "Consumo no horário fora de ponta em kWh.",
    },
    "tarifas": {
        "_table": "Tabela de tarifas de energia por classe de consumo e modalidade tarifária.",
        "tarifa_id": "Identificador único da tarifa.",
        "nome_tarifa": "Nome da modalidade tarifária (Convencional, Branca, Verde, Azul).",
        "classe_consumo": "Classe de consumo à qual a tarifa se aplica.",
        "valor_kwh_reais": "Valor cobrado por kWh consumido, em reais.",
        "valor_demanda_reais": "Valor cobrado por kW de demanda, em reais.",
        "vigencia_inicio": "Data de início de vigência da tarifa.",
    },
    "faturas": {
        "_table": "Faturas mensais emitidas para os clientes, com valores, consumo e status de pagamento.",
        "fatura_id": "Identificador único da fatura.",
        "cliente_id": "Referência ao cliente faturado.",
        "instalacao_id": "Referência à instalação faturada.",
        "tarifa_id": "Tarifa aplicada na fatura.",
        "mes_referencia": "Mês de referência da fatura.",
        "consumo_kwh": "Consumo faturado em kWh.",
        "valor_total_reais": "Valor total da fatura em reais.",
        "status_pagamento": "Situação de pagamento (Paga, Em Aberto, Vencida).",
        "data_vencimento": "Data de vencimento da fatura.",
    },
    "manutencao_equipamentos": {
        "_table": "Histórico de manutenções realizadas nos equipamentos da rede elétrica.",
        "manutencao_id": "Identificador único da ordem de manutenção.",
        "equipamento_id": "Referência ao equipamento que recebeu manutenção.",
        "tipo_manutencao": "Tipo de manutenção (Preventiva, Corretiva, Preditiva).",
        "data_manutencao": "Data de realização da manutenção.",
        "custo_reais": "Custo total da manutenção em reais.",
        "duracao_horas": "Duração da manutenção em horas.",
        "tecnico_responsavel": "Técnico responsável pela execução.",
        "resultado": "Resultado da manutenção (Concluída, Pendente, Reagendada).",
    },
    "interrupcoes": {
        "_table": "Eventos de interrupção no fornecimento de energia, com causa, duração e impacto em clientes.",
        "interrupcao_id": "Identificador único da interrupção.",
        "equipamento_id": "Equipamento associado à interrupção.",
        "instalacao_id": "Instalação associada à interrupção.",
        "tipo_interrupcao": "Tipo da interrupção (Programada, Não Programada, Emergencial).",
        "causa": "Causa raiz da interrupção.",
        "data_inicio": "Data e hora de início da interrupção.",
        "duracao_minutos": "Duração da interrupção em minutos.",
        "clientes_afetados": "Quantidade de clientes afetados pela interrupção.",
    },
}

def esc(s: str) -> str:
    return s.replace("'", "''")

for tabela, cols in docs.items():
    table_comment = cols.get("_table", "")
    if table_comment:
        spark.sql(f"COMMENT ON TABLE {catalog}.{schema}.{tabela} IS '{esc(table_comment)}'")
    for coluna, comentario in cols.items():
        if coluna == "_table":
            continue
        try:
            spark.sql(f"ALTER TABLE {catalog}.{schema}.{tabela} ALTER COLUMN {coluna} COMMENT '{esc(comentario)}'")
        except Exception as e:
            print(f"[aviso] {tabela}.{coluna}: {e}")
print("Documentação aplicada.")

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {catalog}.{schema}"))