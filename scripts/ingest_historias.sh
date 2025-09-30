#!/bin/bash
FECHA_INGESTA=$(date +%Y-%m-%d)
HDFS_DIR="/datalake/raw/clinic/historias_csv/ingestion_date=$FECHA_INGESTA"
LOCAL_DIR="$HOME/BigData_UPAO/Proyecto/DATA_CLINICA_CSV"

distritos=("Callayuc" "Choros" "Cujillo" "Cutervo" "LaRamada" "Pimpingos" "Querocotillo" "SanAndresdeCutervo" "SanLuisdeLucma" "SanJuandeCutervo" "SantaCruz" "SantoDomingodelaCapilla" "SantoTomas" "Socota" "ToribioCasanova")

hdfs dfs -mkdir -p $HDFS_DIR

for DIST in "${distritos[@]}"; do
    FILE="${LOCAL_DIR}/DATA_${DIST}.csv"
    if [ -f "$FILE" ]; then
        echo "Subiendo CSV de $DIST..."
        hdfs dfs -mkdir -p "${HDFS_DIR}/distrito=${DIST}"
        hdfs dfs -put -f "$FILE" "${HDFS_DIR}/distrito=${DIST}/"
    else
        echo "No se encontró archivo para $DIST"
    fi
done
