# ============================================================================
# PIPELINE SPARK DECLARATIVO — CAMADA SILVER: CATÁLOGO DE PRODUTOS
# ============================================================================
# Este arquivo transforma dados da camada Bronze (raw) para a camada Silver
# (limpa e padronizada) dentro da arquitetura de medalhas (Medallion).
#
# Camada Bronze: dados brutos ingestados do PostgreSQL, sem transformações.
# Camada Silver: dados limpos, padronizados e com regras de qualidade aplicadas.
# Camada Gold:   agregações e métricas prontas para consumo (não definida aqui).
# ============================================================================

# --- IMPORTS ---------------------------------------------------------------
# `pipelines as dp`: API do Spark Declarative Pipelines (antigo DLT).
#   Fornece decorators como @dp.table, @dp.expect, etc.
# `functions as F`: Funções do Spark SQL (trim, initcap, when, round, etc.)
#   usadas para transformar colunas dentro de DataFrames.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# --- DEFINIÇÃO DA TABELA (DATASET) ------------------------------------------
# @dp.table() define uma STREAMING TABLE — uma tabela que processa dados
# de forma incremental (streaming), lendo apenas novos dados a cada execução.
# O parâmetro `name` publica a tabela no Unity Catalog no caminho qualificado:
#   catálogo  → retail_q
#   schema    → retail_silver
#   tabela    → product_catalog
# O `comment` descreve a tabela para o catálogo (visível no Unity Catalog).
@dp.table(
    name="retail_q.retail_silver.product_catalog",
    comment="Silver layer product catalog with standardized data and data quality rules"
)

# --- REGRAS DE QUALIDADE DE DADOS (EXPECTATIONS) ---------------------------
# Expectations são regras que validam os dados. Existem 3 tipos:
#   @dp.expect()        → Registra violações como AVISO (não remove nem falha).
#   @dp.expect_or_drop() → Remove linhas que violam a regra (silenciosamente).
#   @dp.expect_or_fail() → Faz o pipeline FALAR se a regra for violada.
#
# Cada regra recebe: (nome_da_regra, expressão_SQL_booleana)
# A expressão é avaliada por linha; se retornar FALSE, a ação é aplicada.

# product_id não pode ser nulo nem vazio → remove a linha se violar
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL AND LENGTH(TRIM(product_id)) > 0")
# product_name não pode ser nulo nem vazio → remove a linha se violar
@dp.expect_or_drop("valid_product_name", "product_name IS NOT NULL AND LENGTH(TRIM(product_name)) > 0")
# category não pode ser nula → apenas avisa (não remove)
@dp.expect("valid_category", "category IS NOT NULL")
# unit_price deve ser positivo → apenas avisa (não remove)
@dp.expect("valid_price", "unit_price > 0")
# lauch_date (data de lançamento) não pode ser nula → remove a linha se violar
@dp.expect_or_drop("valid_launch_date", "launch_date IS NOT NULL")
# supplier_name não pode ser nulo → apenas avisa (não remove)
@dp.expect("valid_supplier", "supplier_name IS NOT NULL")

# --- FUNÇÃO QUE PRODUZ O DATAFRAME -----------------------------------------
# A função deve retornar um DataFrame. Como usamos `spark.readStream`,
# o pipeline entende que esta é uma tabela de streaming (incremental).
def product_catalog():
    return (
        # --- LEITURA DA FONTE (CAMADA BRONZE) ----------------------------------
        # spark.readStream.table(): lê a tabela Bronze em modo streaming,
        # processando apenas os novos dados a cada atualização do pipeline.
        # A tabela de origem é: retail_q.postgres_bronze.product_catalog
        spark.readStream.table("retail_q.postgres_bronze.product_catalog")

        # --- FILTRO INICIAL ---------------------------------------------------
        # Mantém apenas produtos ativos (is_active = True).
        # Filtrar cedo reduz a quantidade de dados processados.
        .filter(F.col("is_active") == True)

        # --- SELEÇÃO E TRANSFORMAÇÃO DE COLUNAS -------------------------------
        # .select() escolhe e transforma as colunas que vão para a Silver.
        # Cada coluna pode ser renomeada (.alias) ou transformada (funções F.).
        .select(
            # product_id: repassado sem transformação (chave de identificação)
            F.col("product_id"),

            # product_name: remove espaços nas pontas (trim) e coloca em
            # formato Título/Capitalizado (initcap) → "notebook gamer" vira "Notebook Gamer"
            F.initcap(F.trim(F.col("product_name"))).alias("product_name"),

            # category: mesma padronização (trim + initcap)
            F.initcap(F.trim(F.col("category"))).alias("category"),

            # subcategory: se existir, aplica trim + initcap;
            # se for nula, preenche com "Unknown" (valor padrão).
            F.when(F.col("subcategory").isNotNull(),
                   F.initcap(F.trim(F.col("subcategory"))))
                .otherwise(F.lit("Unknown")).alias("subcategory"),

            # brand: mesma lógica — se nula, preenche com "Unknown"
            F.when(F.col("brand").isNotNull(),
                   F.initcap(F.trim(F.col("brand"))))
            .otherwise(F.lit("Unknown")).alias("brand"),

            # unit_price: arredonda para 2 casas decimais (padronização monetária)
            F.round(F.col("unit_price"), 2).alias("unit_price"),

            # supplier_name: padronização (trim + initcap)
            F.initcap(F.trim(F.col("supplier_name"))).alias("supplier_name"),

            # launch_date: repassado sem transformação (data de lançamento do produto)
            F.col("launch_date"),

            # product_segment: coluna DERIVADA (não existe na origem).
            # Classifica o produto por faixa de preço usando when/otherwise:
            #   > 50.000  → PREMIUM
            #   > 10.000  → MID_RANGE
            #   demais    → BUDGET
            F.when(F.col("unit_price") > 50000, "PREMIUM")
             .when(F.col("unit_price") > 10000, "MID_RANGE")
             .otherwise("BUDGET")
             .alias("product_segment"),

            # --- COLUNAS DE CONTROLE SCD (Slowly Changing Dimensions) -------------
            # A tabela Bronze usa Auto CDC (Change Data Capture), que adiciona
            # colunas técnicas com prefixo underscore para rastrear mudanças:
            #   _START_AT: quando este registro passou a ser válido (início)
            #   _END_AT:   quando deixou de ser válido (NULL = registro atual/ativo)
            # Aqui renomeamos para nomes mais amigáveis na camada Silver.
            F.col("__START_AT").alias("start_at"),
            F.col("__END_AT").alias("end_at"),

            # is_active (Silver): coluna DERIVADA das colunas SCD.
            # Se _END_AT for NULL → o registro é o vigente (True).
            # Se _END_AT tiver valor → o registro foi substituído (False).
            F.when(F.col("__END_AT").isNull(), F.lit(True)).otherwise(F.lit(False)).alias("is_active"),

            # updated_at: data da última atualização do registro na origem
            F.col("updated_at"),

            # processed_at: carimbo de data/hora do momento em que o
            # pipeline processou esta linha (auditoria / rastreabilidade).
            # current_timestamp() é avaliado no momento do processamento.
            F.current_timestamp().alias("processed_at")
        )
    )