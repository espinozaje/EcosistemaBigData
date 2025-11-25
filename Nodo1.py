import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lower, trim, regexp_replace, to_date, concat, lit, substring


postgres_jar = "/home/espinozaje/jars/postgresql-42.7.3.jar"


spark = SparkSession.builder \
    .appName("Ingesta_Dinamica") \
    .config("spark.driver.memory", "4g") \
    .config("spark.jars", postgres_jar) \
    .getOrCreate()


schema = StructType([
    StructField("DNI", StringType(), True), StructField("Asegurado", StringType(), True),
    StructField("FecNac", StringType(), True), StructField("SEXO", StringType(), True),
    StructField("CPP", StringType(), True), StructField("síntomas", StringType(), True),
    StructField("enfermedad detectada", StringType(), True), StructField("Distrito", StringType(), True),
])


base_path = "hdfs://localhost:9000/datalake/raw/clinic/historias_csv"


fecha_hoy = datetime.today().strftime('%Y-%m-%d')
carpeta_fecha = f"ingestion_date={fecha_hoy}"


ruta_dinamica = f"{base_path}/{carpeta_fecha}/*/*.csv"

print(f"Buscando archivos en: {ruta_dinamica}")

try:
    
    df_raw = spark.read.csv(ruta_dinamica, header=True, schema=schema)
    
    total_registros = df_raw.count()
    print(f"Se han leído {total_registros} registros de todos los distritos encontrados.")

    if total_registros > 0:
        
        df_clean = df_raw \
            .withColumn("FecNac", to_date(col("FecNac"), "yyyy-MM-dd")) \
            .fillna({"síntomas": "DESCONOCIDOS", "enfermedad detectada": "DESCONOCIDA"}) \
            .dropDuplicates(["DNI"]) \
            .withColumn("DNI_masked", concat(lit("XXX-"), substring(col("DNI"), 5, 4))) \
            .drop("DNI")

        
        IP_VM1 = "100.68.144.113"
        db_url = f"jdbc:postgresql://{IP_VM1}:5432/postgres"

        print(f"Enviando a BD Puente ({IP_VM1})...")
        
        df_clean.write \
            .format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", "pacientes_buffer") \
            .option("user", "postgres") \
            .option("password", "admin") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
            
        print("¡ÉXITO TOTAL! Ingesta finalizada.")
    else:
        print("⚠️ La carpeta existe pero no tiene datos.")

except Exception as e:
    print(f"❌ Error: No se encontraron datos para la fecha de hoy ({fecha_hoy}) o hubo un fallo de conexión.")
    print(f"Detalle: {e}")