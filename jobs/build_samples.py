# Transformando os dados em RAW para a camada bronze
# Vou ler os dados na camada RAW e salvar como PARQUET na camada bronze
# Os campos estão como JSON ou CSV e preciso organizar no formato colunar, comprimido e eficiente

# Importa o SparkSession e funções SQL do PySpark como F.
from pyspark.sql import SparkSession, functions as F

# Função utilitária para perfil rápido
# name = para imprimir identificação
# max_cols = limita a quantidade de colunas analisadas para evitar travar em tabelas largas
# n = df.count() => conta as linhas do df. Essa ação vem depois do .limit(100_000), então esse número retorna o dado do df já limitado
# Loop for:
## Para cada coluna faz um agg pedindo dois números: quantos não-nulos e quantos distintos
def quick_profile(df, name: str, max_cols: int = 10):
    n = df.count()
    print(f"\n### {name}: {n} linhas") # Log simples com o total de linhas analisadas
    cols = df.columns[:max_cols] # Pega até max_cols que é uma estratégia barata e suficiente para entender os dados
    for c in cols:
        stats = (df
                 .agg(F.count(F.col(c)).alias("non_null"), # Conta não-nulos, nulos não entram
                      F.countDistinct(F.col(c)).alias("distinct"))
                 .collect()[0]) # Traz só essa linha de agregados para o driver (seguro, é uma linha)
        non_null, distinct = stats["non_null"], stats["distinct"] # Calcula nulos por diferença e imprime. Quando a coluna tem cardinalidade gigante: usar F.approx_count_distinct para ser mais leve.
        nulls = n - non_null
        print(f"- {c}: nulls={nulls}, distinct={distinct}")


# Ponto de entrada do Job.
# appName: nome da aplicação (aparece na Spark UI/logs)
# master("local[*]"): roda local usando todos os núcleos - perfeito para desenvolvimento
# spark.sql.adaptive.enabled=true: ativa AQE (Adaptive Query Execution), que ajuda o Spark a otimizar joins/partitions em tempo de execução.
def main():
    spark = (SparkSession.builder
             .appName("build_samples_dev")
             .master("local[*]")
             .config("spark.sql.adaptive.enabled", "true")
             .getOrCreate())

    # Caminhos dos arquivos
    paths = {
        "order": "data/raw/order/order.json",
        "consumer": "data/raw/consumer/consumer.csv",
        "restaurant": "data/raw/restaurant/restaurant.csv",
        "ab_test": "data/raw/ab_test_ref/ab_test_ref.csv"
    }

    # Leitura permissiva (descoberta); CSV com header e inferência só para esta fase.
    # Lê o JSON com mode = PERMISSIVE: linhas quebradas não derrubam o job (entram null/_corrupt_record)
    # Ótimo para descoberta - depois em produção, vou fixar o schema e deixar quebrar caso o padrão não seja o correto
    df_order = (spark.read.option("mode","PERMISSIVE").json(paths["order"]))
    # header=true: primeira linha é cabeçalho.
    # inferSchema=true: deixa o Spark adivinhar tipos (bom para descoberta; em produção evite, use schema fixo).
    # mode=PERMISSIVE: tolerante a problemas de parsing.
    csv = (spark.read.option("header","true").option("inferSchema","true").option("mode","PERMISSIVE"))
    df_consumer = csv.csv(paths["consumer"])
    df_restaurant = csv.csv(paths["restaurant"])
    df_ab_test = csv.csv(paths["ab_test"])

    # Amostras determinísticas (~1% ou mínimo 100k linhas se existirem)
    frac = 0.01
    seed = 42
    # Gera DataFrames amostrados (ainda Lazy, nada executou)
    # withReplacement = False: cada linha entra no máximo uma vez (sem reposição)
    samples = {
        "order_sample": df_order.sample(withReplacement=False, fraction=frac, seed=seed),
        "consumer_sample": df_consumer.sample(False, frac, seed),
        "restaurant_sample": df_restaurant.sample(False, frac, seed),
        "ab_test_sample": df_ab_test.sample(False, frac, seed)
    }

    # Cria uma pasta dedicada para iteração rápida
    out_base = "data/processed/samples"
    for name, df in samples.items():
        # materializa a amostra (lazy → ação controlada)
        df = df.limit(100_000)  # teto de 100k p/ evitar estouro local
        df.write.mode("overwrite").parquet(f"{out_base}/{name}")
        print(f"✔︎ gravado {out_base}/{name} (parquet)")

        # Perfil rápido (contagem, nulos, distinct de algumas colunas)
        quick_profile(df, name)

    spark.stop()

# Executa main() quando rodar arquivo diretamente
if __name__ == "__main__":
    main()
