"""
demo_backfill — a demo ao vivo da Aula 4 (slide "Backfill em 90 segundos")

Três diferenças em relação à DAG principal:
  1. schedule="@daily"          -> existe um calendário
  2. start_date no passado      -> qualquer janela de backfill funciona
  3. a task lê logical_date     -> "que dia eu sou?"

Como demonstrar (Airflow 3): abrir a DAG -> Trigger ▾ -> Backfill ->
data inicial = 7 dias atrás, data final = ontem -> "não reprocessar existentes" -> confirmar.
Sete execuções aparecem na Grid; o log de cada uma imprime uma data diferente.

Desafio Prata B: aplique as mesmas 3 mudanças na pipeline_olist_completo.
"""
from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="demo_backfill",
    description="Demo: a task não sabe 'hoje', ela sabe logical_date (Aula 4 FIAP)",
    schedule="@daily",                 # 1. tem schedule
    start_date=datetime(2026, 1, 1),   # 2. no passado — sem editar o código a cada aula
    catchup=False,                     # não sair reprocessando desde janeiro sozinho
    tags=["fiap", "aula4", "demo"],
)
def demo():
    @task
    def que_dia_eu_sou(**context) -> str:
        # 3. a task recebe do orquestrador a data que ela representa.
        # Num backfill rodado hoje para 21/08, logical_date é 21/08 — não "hoje".
        d = context["logical_date"] or context["dag_run"].run_after
        dia = f"{d:%Y-%m-%d}"
        print(f"Processando o dia {dia}")
        print("Aqui entraria: baixar SÓ os pedidos deste dia, transformar, validar.")
        return dia

    que_dia_eu_sou()


demo()