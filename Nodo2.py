#!/usr/bin/env python
# coding: utf-8

import os
import sys
import shutil
import gc

# --- SPARK & MLlib IMPORTS ---
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, regexp_replace, monotonically_increasing_id, row_number, split, explode, collect_list, concat_ws, size, array, expr
from pyspark.sql.window import Window
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, Word2Vec, BucketedRandomProjectionLSH

# --- CONFIGURACIÓN ---
PYTHON_VENV_PATH = "/home/espinozaje/BigData_UPAO/bigdata_env/bin/python3"
os.environ['PYSPARK_PYTHON'] = PYTHON_VENV_PATH
os.environ['PYSPARK_DRIVER_PYTHON'] = PYTHON_VENV_PATH
postgres_jar = "/home/espinozaje/jars/postgresql-42.7.3.jar"

# 1. INICIAR SPARK (Ajustado para estabilidad en VM Local)
# Bajamos la memoria a 4g/4g para evitar que el SO mate el proceso por OOM
spark = SparkSession.builder \
    .appName("Nodo_IA_PRO_LSH_Stable") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.jars", postgres_jar) \
    .config("spark.driver.extraClassPath", postgres_jar) \
    .config("spark.sql.shuffle.partitions", "20") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR") # Menos ruido en los logs
print("🚀 Nodo IA (LSH Optimizado) iniciado.")

# 2. LECTURA DE DATOS
print("📥 Leyendo datos de PostgreSQL...")
db_url = "jdbc:postgresql://localhost:5432/data_clinica"
df_pre = spark.read.format("jdbc") \
    .option("url", db_url) \
    .option("dbtable", "pacientes_buffer") \
    .option("user", "postgres") \
    .option("password", "admin") \
    .load()

# --- CORRECCIÓN CLAVE: Generación de ID Distribuida ---
# Usamos monotonically_increasing_id() en lugar de Window
df_pacientes = df_pre.withColumn("id_unico", monotonically_increasing_id()).cache()

# Cargar Diccionario
ruta_diccionario = "file:///home/espinozaje/BigData_UPAO/Proyecto/CIE10_2021.csv"
df_diccionario = spark.read.option("encoding", "ISO-8859-1").csv(ruta_diccionario, header=True, inferSchema=True) \
    .dropna(subset=['DESCRIPCION', 'CODIGO']) \
    .select(col("CODIGO").alias("CIE_COD"), col("DESCRIPCION").alias("CIE_DESC"))

print(f"📊 Datos Pacientes: {df_pacientes.count()} | Diccionario Cargado.")

# 3. LIMPIEZA DE TEXTO (NATIVA)
def limpiar_texto_nativo(df, col_input, col_output="texto_limpio"):
    return df.withColumn(col_output, lower(trim(col(col_input)))) \
        .withColumn(col_output, regexp_replace(col(col_output), "[^a-z0-9áéíóúñ ]", "")) \
        .withColumn(col_output, regexp_replace(col(col_output), "sensibilidad a la luz", "alteraciones visuales subjetivas")) \
        .withColumn(col_output, regexp_replace(col(col_output), "molestia a la luz", "alteraciones visuales subjetivas")) \
        .withColumn(col_output, regexp_replace(col(col_output), "fotofobia", "alteraciones visuales subjetivas")) \
        .withColumn(col_output, regexp_replace(col(col_output), "visión borrosa", "alteraciones visuales subjetivas")) \
        .withColumn(col_output, regexp_replace(col(col_output), "vision borrosa", "alteraciones visuales subjetivas")) \
        .withColumn(col_output, regexp_replace(col(col_output), "ojos llorosos", "epífora")) \
        .withColumn(col_output, regexp_replace(col(col_output), "lagrimeo", "epífora")) \
        .withColumn(col_output, regexp_replace(col(col_output), "dificultad para respirar", "disnea")) \
        .withColumn(col_output, regexp_replace(col(col_output), "falta de aire", "disnea")) \
        .withColumn(col_output, regexp_replace(col(col_output), "ahogo", "disnea")) \
        .withColumn(col_output, regexp_replace(col(col_output), "sibilancias", "silbido")) \
        .withColumn(col_output, regexp_replace(col(col_output), "le silba el pecho", "silbido")) \
        .withColumn(col_output, regexp_replace(col(col_output), "opresión en el pecho", "dolor en el pecho")) \
        .withColumn(col_output, regexp_replace(col(col_output), "opresion en el pecho", "dolor en el pecho")) \
        .withColumn(col_output, regexp_replace(col(col_output), "estornudos", "estornudo")) \
        .withColumn(col_output, regexp_replace(col(col_output), "congestión nasal", "otros transtornos especificados de la nariz y los senos paranasales")) \
        .withColumn(col_output, regexp_replace(col(col_output), "congestion nasal", "otros transtornos especificados de la nariz y los senos paranasales")) \
        .withColumn(col_output, regexp_replace(col(col_output), "tos seca", "tos")) \
        .withColumn(col_output, regexp_replace(col(col_output), "acidez estomacal", "acidez")) \
        .withColumn(col_output, regexp_replace(col(col_output), "ardor de estomago", "acidez")) \
        .withColumn(col_output, regexp_replace(col(col_output), "dolor abdominal", "otros dolores abdominales y los no especificados")) \
        .withColumn(col_output, regexp_replace(col(col_output), "dolor de barriga", "otros dolores abdominales y los no especificados")) \
        .withColumn(col_output, regexp_replace(col(col_output), "náuseas", "náusea y vómito")) \
        .withColumn(col_output, regexp_replace(col(col_output), "nauseas", "náusea y vómito")) \
        .withColumn(col_output, regexp_replace(col(col_output), "vómitos", "náusea y vómito")) \
        .withColumn(col_output, regexp_replace(col(col_output), "vomitos", "náusea y vómito")) \
        .withColumn(col_output, regexp_replace(col(col_output), "dolor de cabeza", "cefalea")) \
        .withColumn(col_output, regexp_replace(col(col_output), "cansancio", "malestar y fatiga")) \
        .withColumn(col_output, regexp_replace(col(col_output), "fatiga", "malestar y fatiga")) \
        .withColumn(col_output, regexp_replace(col(col_output), "micción frecuente", "poliuria")) \
        .withColumn(col_output, regexp_replace(col(col_output), "miccion frecuente", "poliuria")) \
        .withColumn(col_output, regexp_replace(col(col_output), "sed excesiva", "polidipsia")) \
        .withColumn(col_output, regexp_replace(col(col_output), "pérdida de olfato", "anosmia")) \
        .withColumn(col_output, regexp_replace(col(col_output), "perdida de olfato", "anosmia")) \
        .withColumn(col_output, regexp_replace(col(col_output), "picazón", "prurito no especificado")) \
        .withColumn(col_output, regexp_replace(col(col_output), "picazon", "prurito no especificado")) \
        .withColumn(col_output, regexp_replace(col(col_output), "mareos", "mareo y desvanecimiento")) \
        .withColumn(col_output, regexp_replace(col(col_output), "^diabetes$", "diabetes mellitus no especificada")) \
        .withColumn(col_output, regexp_replace(col(col_output), "^azucar alta$", "diabetes mellitus no especificada")) \
        .withColumn(col_output, regexp_replace(col(col_output), "^hipertensión$", "hipertension esencial primaria")) \
        .withColumn(col_output, regexp_replace(col(col_output), "^hipertension$", "hipertension esencial primaria")) \
        .withColumn(col_output, regexp_replace(col(col_output), "^presion alta$", "hipertension esencial primaria")) \
        .withColumn(col_output, regexp_replace(col(col_output), "^alergia$", "alergia no especificada")) \
        .withColumn(col_output, regexp_replace(col(col_output), "^asma$", "asma no especificada")) \
        .withColumn(col_output, regexp_replace(col(col_output), "^gripe$", "influenza con otras manifestaciones respiratorias virus no identificado")) \
        .filter(col(col_output).isNotNull())

# 4. PIPELINE DE NLP (Optimizado)
print("⚙️ Construyendo Pipeline Spark ML...")

tokenizer = Tokenizer(inputCol="texto_limpio", outputCol="tokens")
remover = StopWordsRemover(inputCol="tokens", outputCol="tokens_filtered", stopWords=["de", "la", "el", "en", "y", "o", "un", "una", "con", "del", "los", "las", "al", "su", "que", "es", "son", "se", "mi", "tu"])

word2Vec = Word2Vec(vectorSize=50, minCount=1, inputCol="tokens_filtered", outputCol="features", maxIter=10, stepSize=0.025)

# LSH ADJUSTE: bucketLength más grande = menos memoria, pero menos preciso. 
# numHashTables=1 = menos memoria.
brp = BucketedRandomProjectionLSH(inputCol="features", outputCol="hashes", bucketLength=10.0, numHashTables=1)

df_dic_clean = limpiar_texto_nativo(df_diccionario, "CIE_DESC")
df_pac_enf = limpiar_texto_nativo(df_pacientes, "enfermedad detectada")
df_corpus = df_dic_clean.select("texto_limpio").union(df_pac_enf.select("texto_limpio"))

pipeline_w2v = Pipeline(stages=[tokenizer, remover, word2Vec])
model_w2v = pipeline_w2v.fit(df_corpus)

df_dic_vec = model_w2v.transform(df_dic_clean)
model_lsh = brp.fit(df_dic_vec)

print("✅ Modelos Entrenados. Iniciando Matching...")

# 5. PROCESO DE MATCHING (LSH)

# A) ENFERMEDADES
df_pac_enf_vec = model_w2v.transform(df_pac_enf)
print("   -> Cruzando Enfermedades...")
# Join aproximado
match_enf = model_lsh.approxSimilarityJoin(df_pac_enf_vec, df_dic_vec, threshold=2.0, distCol="distancia")

# Filtrar mejor match (Distribuido)
w_rank = Window.partitionBy("datasetA.id_unico").orderBy("distancia")
best_match_enf = match_enf.withColumn("rank", row_number().over(w_rank)) \
    .filter(col("rank") == 1) \
    .select(col("datasetA.id_unico").alias("id_unico"), 
            col("datasetB.CIE_COD").alias("COD_ENFERMEDAD"), 
            col("datasetB.CIE_DESC").alias("DESC_ENFERMEDAD"))

# B) SÍNTOMAS
print("   -> Cruzando Síntomas...")
df_sintomas = df_pacientes.select("id_unico", explode(split(col("síntomas"), ",")).alias("sintoma_individual"))
df_sint_clean = limpiar_texto_nativo(df_sintomas, "sintoma_individual")
df_sint_vec = model_w2v.transform(df_sint_clean)

match_sint = model_lsh.approxSimilarityJoin(df_sint_vec, df_dic_vec, threshold=2.0, distCol="distancia")

best_match_sint = match_sint.withColumn("rank", row_number().over(Window.partitionBy("datasetA.id_unico", "datasetA.sintoma_individual").orderBy("distancia"))) \
    .filter(col("rank") == 1) \
    .select(col("datasetA.id_unico"), col("datasetB.CIE_COD"), col("datasetB.CIE_DESC"))

final_sint = best_match_sint.groupBy("id_unico").agg(
    concat_ws(", ", collect_list("CIE_COD")).alias("codigos_sintomas"),
    concat_ws(" | ", collect_list("CIE_DESC")).alias("descripcion_sintomas")
)

# 6. UNIFICACIÓN
print("🔗 Unificando resultados...")
df_final = df_pacientes.join(best_match_enf, "id_unico", "left") \
    .join(final_sint, "id_unico", "left") \
    .fillna({"COD_ENFERMEDAD": "DESCONOCIDA", "DESC_ENFERMEDAD": "DESCONOCIDA", 
             "codigos_sintomas": "DESCONOCIDOS", "descripcion_sintomas": "DESCONOCIDOS"})

# Guardado intermedio en Parquet (Esto rompe el linaje y libera memoria)
output_path = "resultado_final_lsh"
try: shutil.rmtree(output_path)
except: pass

df_final.write.mode("overwrite").parquet(output_path)
print("✅ Guardado en Parquet exitoso.")

# 7. EXPORTACIÓN A POSTGRESQL
print("📤 Exportando a PostgreSQL...")
IP_NODO3 = '100.68.144.113' # Asegúrate que esta IP es correcta
jdbc_url_export = f"jdbc:postgresql://{IP_NODO3}:5432/data_clinica"

# Leemos del parquet recién creado para subir a BD
df_export_final = spark.read.parquet(output_path).select(
    col("Asegurado"), col("FecNac"), col("SEXO"), col("Distrito"),
    col("síntomas").alias("sintomas_original"),
    col("codigos_sintomas"),
    col("descripcion_sintomas"),
    col("enfermedad detectada").alias("enfermedad_original"),
    col("DESC_ENFERMEDAD").alias("descripcion_enfermedad"),
    col("COD_ENFERMEDAD").alias("codigo_enfermedad")
)

try:
    df_export_final.write.jdbc(url=jdbc_url_export, table="resultados_cie10", mode="overwrite", properties={"user": "postgres", "password": "admin", "driver": "org.postgresql.Driver"})
    print("🎉 ¡EXPORTACIÓN EXITOSA!")
except Exception as e:
    print(f"❌ Error exportando BD: {e}")

spark.stop()