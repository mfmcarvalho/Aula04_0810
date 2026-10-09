"""
pipeline_olist_completo — Aula 4 (Airflow 3.2 no Astro)
FIAP MBA Data Engineering · Data Integration e Pipelines · Prof. Rafael S Novo Pereira

GitHub (9 CSVs do Olist, 7 usados)
   → download_and_load_bronze   (7 tabelas em OLIST_LAB.PUBLIC.BRONZE_*)
   → transform_silver           (OLIST_LAB.PUBLIC.SILVER_PEDIDOS)
   → build_gold                 (GOLD_RECEITA_ESTADO, GOLD_VENDAS_MENSAL)
   → quality_checks             (4 regras; se uma falha, "fim" não roda)
   → fim

Pré-requisito: Connection `snowflake_default` (Etapa 13 do Guia Visual).
A senha NUNCA fica no código — só na Connection, criptografada no Metadata DB.

Idempotência: Bronze usa overwrite=True; Silver e Gold usam CREATE OR REPLACE.
Rodar duas vezes produz o mesmo resultado.
"""
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator

CONN_ID = "snowflake_default"
DB = "OLIST_LAB"
SCHEMA = "PUBLIC"
BASE_URL = (
    "https://raw.githubusercontent.com/"
    "mfmcarvalho/Aula04_0810/main/datasets/")

# 7 dos 9 CSVs do repositório (geolocation e a tradução de categorias ficam de fora: não entram em nenhuma métrica)
ARQUIVOS = {
    "BRONZE_ORDERS":      "olist_orders_dataset.csv",
    "BRONZE_ORDER_ITEMS": "olist_order_items_dataset.csv",
    "BRONZE_CUSTOMERS":   "olist_customers_dataset.csv",
    "BRONZE_PRODUCTS":    "olist_products_dataset.csv",
    "BRONZE_SELLERS":     "olist_sellers_dataset.csv",
    "BRONZE_PAYMENTS":    "olist_order_payments_dataset.csv",
    "BRONZE_REVIEWS":     "olist_order_reviews_dataset.csv",
}


def _hook():
    # import dentro da função: o DAG Processor não precisa do provider para montar o grafo
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    return SnowflakeHook(snowflake_conn_id=CONN_ID)


@dag(
    dag_id="pipeline_olist_completo",
    description="Olist: GitHub → Bronze → Silver → Gold → quality checks (Aula 4 FIAP)",
    schedule=None,                      # manual. Desafio Prata B: "@daily"
    start_date=datetime(2025, 1, 1),
    catchup=False,                      # sem isso o Airflow tentaria rodar todos os dias desde 2025
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["fiap", "olist", "aula4"],
)
def pipeline():
    inicio = EmptyOperator(task_id="inicio")
    fim = EmptyOperator(task_id="fim")

    # ------------------------------------------------------------------ BRONZE
    @task
    def download_and_load_bronze() -> dict:
        """Baixa os CSVs do GitHub e grava 7 tabelas Bronze (cópia fiel, overwrite).

        Download e load ficam na MESMA task de propósito: no Astro cada task roda
        num pod Kubernetes isolado — um arquivo salvo em /tmp por uma task não
        existe para a próxima. (Ver slide "A prova real".)
        """
        import pandas as pd
        from snowflake.connector.pandas_tools import write_pandas

        conn = _hook().get_conn()
        cur = conn.cursor()
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {DB}")
        cur.execute(f"USE DATABASE {DB}")
        cur.execute(f"USE SCHEMA {SCHEMA}")

        linhas = {}
        for tabela, arquivo in ARQUIVOS.items():
            df = pd.read_csv(BASE_URL + arquivo)
            df.columns = [c.upper() for c in df.columns]
            write_pandas(
                conn, df, tabela,
                database=DB, schema=SCHEMA,
                auto_create_table=True, overwrite=True,
            )
            linhas[tabela] = len(df)
            print(f"[BRONZE] {tabela:<20} {len(df):>8} linhas  <- {arquivo}")
        cur.close()
        conn.close()
        return linhas  # vai para o XCom (valores pequenos podem viajar entre tasks)

    # ------------------------------------------------------------------ SILVER
    @task
    def transform_silver() -> int:
        """1 tabela: pedido + cliente + pagamentos (soma) + itens (soma), tipada."""
        hook = _hook()
        hook.run(f"""
            CREATE OR REPLACE TABLE {DB}.{SCHEMA}.SILVER_PEDIDOS AS
            SELECT
                o.ORDER_ID                                              AS PEDIDO_ID,
                o.CUSTOMER_ID                                           AS CLIENTE_ID,
                c.CUSTOMER_STATE                                        AS ESTADO,
                c.CUSTOMER_CITY                                         AS CIDADE,
                o.ORDER_STATUS                                          AS STATUS,
                TRY_TO_TIMESTAMP(o.ORDER_PURCHASE_TIMESTAMP)            AS DT_COMPRA,
                TRY_TO_TIMESTAMP(o.ORDER_DELIVERED_CUSTOMER_DATE)       AS DT_ENTREGA,
                TRY_TO_TIMESTAMP(o.ORDER_ESTIMATED_DELIVERY_DATE)       AS DT_ENTREGA_PREVISTA,
                COALESCE(p.VALOR_PAGO, 0)                               AS VALOR_PAGO,
                COALESCE(i.QTD_ITENS, 0)                                AS QTD_ITENS,
                COALESCE(i.VALOR_ITENS, 0)                              AS VALOR_ITENS,
                COALESCE(i.VALOR_FRETE, 0)                              AS VALOR_FRETE
            FROM {DB}.{SCHEMA}.BRONZE_ORDERS o
            JOIN {DB}.{SCHEMA}.BRONZE_CUSTOMERS c
                 ON c.CUSTOMER_ID = o.CUSTOMER_ID
            LEFT JOIN (
                SELECT ORDER_ID, SUM(PAYMENT_VALUE) AS VALOR_PAGO
                FROM {DB}.{SCHEMA}.BRONZE_PAYMENTS GROUP BY ORDER_ID
            ) p ON p.ORDER_ID = o.ORDER_ID
            LEFT JOIN (
                SELECT ORDER_ID,
                       COUNT(*)           AS QTD_ITENS,
                       SUM(PRICE)         AS VALOR_ITENS,
                       SUM(FREIGHT_VALUE) AS VALOR_FRETE
                FROM {DB}.{SCHEMA}.BRONZE_ORDER_ITEMS GROUP BY ORDER_ID
            ) i ON i.ORDER_ID = o.ORDER_ID
        """)
        total = hook.get_first(f"SELECT COUNT(*) FROM {DB}.{SCHEMA}.SILVER_PEDIDOS")[0]
        print(f"[SILVER] SILVER_PEDIDOS: {total} pedidos")
        return int(total)

    # ------------------------------------------------------------------ GOLD
    @task
    def build_gold() -> int:
        """2 tabelas prontas para BI: receita por estado e vendas por mês."""
        hook = _hook()
        hook.run(f"""
            CREATE OR REPLACE TABLE {DB}.{SCHEMA}.GOLD_RECEITA_ESTADO AS
            SELECT
                ESTADO,
                COUNT(*)                                                     AS TOTAL_PEDIDOS,
                ROUND(SUM(VALOR_PAGO), 2)                                    AS RECEITA_TOTAL,
                ROUND(SUM(VALOR_PAGO) / COUNT(*), 2)                         AS TICKET_MEDIO,
                ROUND(AVG(DATEDIFF('day', DT_COMPRA, DT_ENTREGA)), 1)        AS DIAS_ENTREGA_MEDIO,
                ROUND(100.0 * SUM(IFF(DT_ENTREGA > DT_ENTREGA_PREVISTA, 1, 0))
                            / COUNT(*), 2)                                   AS PCT_ATRASO,
                CURRENT_TIMESTAMP()                                          AS _ATUALIZADO_EM
            FROM {DB}.{SCHEMA}.SILVER_PEDIDOS
            GROUP BY ESTADO
            ORDER BY RECEITA_TOTAL DESC
        """)
        hook.run(f"""
            CREATE OR REPLACE TABLE {DB}.{SCHEMA}.GOLD_VENDAS_MENSAL AS
            SELECT
                DATE_TRUNC('month', DT_COMPRA)::DATE   AS MES,
                COUNT(*)                               AS TOTAL_PEDIDOS,
                ROUND(SUM(VALOR_PAGO), 2)              AS RECEITA_TOTAL,
                ROUND(SUM(VALOR_FRETE), 2)             AS FRETE_TOTAL,
                CURRENT_TIMESTAMP()                    AS _ATUALIZADO_EM
            FROM {DB}.{SCHEMA}.SILVER_PEDIDOS
            WHERE DT_COMPRA IS NOT NULL
            GROUP BY 1
            ORDER BY 1
        """)
        estados = hook.get_first(f"SELECT COUNT(*) FROM {DB}.{SCHEMA}.GOLD_RECEITA_ESTADO")[0]
        print(f"[GOLD] GOLD_RECEITA_ESTADO: {estados} estados | GOLD_VENDAS_MENSAL criada")
        return int(estados)

    # ------------------------------------------------------------------ QUALITY
    @task
    def quality_checks(total_silver: int) -> None:
        """O palito do bolo. Se qualquer regra falhar, a task falha e 'fim' não roda.

        Desafio Prata A: adicione uma regra que FALHE de verdade, ex.:
            ("Nenhum estado com atraso > 10%",
             f"SELECT COUNT(*) FROM {gold} WHERE PCT_ATRASO > 10", lambda v: v == 0),
        """
        hook = _hook()
        gold = f"{DB}.{SCHEMA}.GOLD_RECEITA_ESTADO"
        regras = [
            # (descrição, query que devolve 1 número, função que diz se o valor é aceitável)
            ("Gold não está vazia",
             f"SELECT COUNT(*) FROM {gold}", lambda v: v > 0),
            ("Nenhuma receita negativa",
             f"SELECT COUNT(*) FROM {gold} WHERE RECEITA_TOTAL < 0", lambda v: v == 0),
            ("PCT_ATRASO entre 0 e 100",
             f"SELECT COUNT(*) FROM {gold} WHERE PCT_ATRASO < 0 OR PCT_ATRASO > 100", lambda v: v == 0),
            ("Soma de pedidos na Gold == pedidos na Silver (reconciliação)",
             f"SELECT SUM(TOTAL_PEDIDOS) FROM {gold}", lambda v: v == total_silver),
        ]
        falhas = []
        for descricao, sql, aceitavel in regras:
            valor = hook.get_first(sql)[0]
            status = "OK  " if aceitavel(valor) else "FAIL"
            print(f"[QUALITY] {status} {descricao:<62} valor={valor}")
            if status == "FAIL":
                falhas.append(descricao)

        print("[QUALITY] Top 5 estados por receita:")
        for estado, pedidos, receita, atraso in hook.get_records(
            f"SELECT ESTADO, TOTAL_PEDIDOS, RECEITA_TOTAL, PCT_ATRASO FROM {gold} "
            f"ORDER BY RECEITA_TOTAL DESC LIMIT 5"
        ):
            print(f"          {estado}  pedidos={pedidos:>6}  receita=R$ {float(receita):>12,.2f}  atraso={atraso}%")

        if falhas:
            raise ValueError(f"Quality check falhou: {falhas}. A Gold NÃO deve ser servida.")

    # ------------------------------------------------------------------ ORDEM
    bronze = download_and_load_bronze()
    silver = transform_silver()
    gold = build_gold()
    qc = quality_checks(silver)

    inicio >> bronze >> silver >> gold >> qc >> fim


pipeline()