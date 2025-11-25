#!/bin/bash

# 1. Configuración (Recibe la carpeta local como argumento)
LOCAL_SOURCE_DIR="$1"
FECHA_INGESTA=$(date +%Y-%m-%d)
# Nota: Si HDFS está en otra máquina, asegúrate de que core-site.xml tenga la IP o usa hdfs://IP:9000
HDFS_BASE_DIR="/datalake/raw/clinic/historias_csv/ingestion_date=$FECHA_INGESTA"

echo "------------------------------------------------"
echo "Iniciando Carga a HDFS (Data Lake)..."
echo "Origen Local: $LOCAL_SOURCE_DIR"
echo "Destino HDFS: $HDFS_BASE_DIR"
echo "------------------------------------------------"


hdfs dfs -mkdir -p "$HDFS_BASE_DIR"


count=0
for filepath in "$LOCAL_SOURCE_DIR"/*.csv; do
    if [ -f "$filepath" ]; then
        
        filename=$(basename -- "$filepath")
        
        
        distrito="${filename#DATA_}"
        distrito="${distrito%.csv}"
        
        echo " Procesando: $distrito"
        
        # Crear carpeta del distrito en HDFS
        hdfs dfs -mkdir -p "$HDFS_BASE_DIR/distrito=$distrito"
        
        # Subir el archivo
        hdfs dfs -put -f "$filepath" "$HDFS_BASE_DIR/distrito=$distrito/"
        
        ((count++))
    fi
done

echo "------------------------------------------------"
echo "Carga completada. $count archivos subidos al Data Lake."
echo "------------------------------------------------"