import streamlit as st
import os
import subprocess
import paramiko
import shutil
from scp import SCPClient 


IP_LOURDES = "100.117.253.21" 
USER_AMIGO1 = "lourdes2204"
PASS_AMIGO1 = "upao"
PYTHON_BIN_AMIGO1 = "/home/lourdes2204/BigData_UPAO/bigdata_env/bin/python3" 

RUTAS_AMIGO1 = {
    "CARPETA_CSV": "/home/lourdes2204/BigData_UPAO/Proyecto/DATA_CLINICA_CSV", 
    "SCRIPT_SH": "/home/lourdes2204/BigData_UPAO/Proyecto/scripts/subir_hdfs.sh",
    "SCRIPT_NODO1": "/home/lourdes2204/BigData_UPAO/Proyecto/Nodo1.py"
}

RUTAS_LOCALES = {
    "TEMP_UPLOAD": "./temp_uploads", 
    "PYTHON_LOCAL": "/home/espinozaje/BigData_UPAO/bigdata_env/bin/python3",
    "SCRIPT_NODO2": "/home/espinozaje/BigData_UPAO/Proyecto/Nodo2.py"
}

URL_GRAFANA = "https://gsanchezdebian.tailcd2742.ts.net"

def crear_cliente_ssh(ip, user, password):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(ip, username=user, password=password)
    return client

def transferir_archivos_sftp(ssh_client, local_path, remote_path):
    sftp = ssh_client.open_sftp()
    
    
    try:
        sftp.stat(remote_path)
    except FileNotFoundError:
        sftp.mkdir(remote_path)

    # Subir archivos
    archivos = os.listdir(local_path)
    for archivo in archivos:
        local_file = os.path.join(local_path, archivo)
        remote_file = os.path.join(remote_path, archivo)
        sftp.put(local_file, remote_file)
    
    sftp.close()


st.set_page_config(page_title="Portal Clinico", page_icon="🏥", layout="centered")

st.title("🏥 Portal de Inteligencia Clinica Big data")
st.markdown(f"""
* **Angulo Renteria Lourdes
* **Cacho Quispe Yuliana
* **Espinoza Eche Jeisson
* **Medina Rodriguez Khatia
* **Sanchez Castro Giampiero
""")
st.divider()


st.header("1. Ingesta de Datos")
uploaded_files = st.file_uploader("Sube los CSV aquí", type="csv", accept_multiple_files=True)

if st.button("INICIAR PIPELINE DISTRIBUIDO", type="primary", disabled=not uploaded_files):
    
    progress_bar = st.progress(0, text="Iniciando orquestación...")
    status_text = st.empty()
    ssh_client = None

    try:
        
        status_text.markdown("### 1. Procesando subida web...")
        
        if os.path.exists(RUTAS_LOCALES["TEMP_UPLOAD"]):
            shutil.rmtree(RUTAS_LOCALES["TEMP_UPLOAD"])
        os.makedirs(RUTAS_LOCALES["TEMP_UPLOAD"], exist_ok=True)

        for archivo in uploaded_files:
            with open(os.path.join(RUTAS_LOCALES["TEMP_UPLOAD"], archivo.name), "wb") as f:
                f.write(archivo.getbuffer())
        
        progress_bar.progress(10, text="Archivos cacheados localmente.")

        
        status_text.markdown(f"### 2. Enviando datos...")
        
        ssh_client = crear_cliente_ssh(IP_LOURDES, USER_AMIGO1, PASS_AMIGO1)
        
        transferir_archivos_sftp(
            ssh_client, 
            RUTAS_LOCALES["TEMP_UPLOAD"], 
            RUTAS_AMIGO1["CARPETA_CSV"]
        )
        
        progress_bar.progress(30, text="Transferencia SFTP completada.")

        
        status_text.markdown("### 3. Ejecutando Ingesta Remota (HDFS + Spark)...")

        
        cmd_hdfs = f"bash {RUTAS_AMIGO1['SCRIPT_SH']} {RUTAS_AMIGO1['CARPETA_CSV']}"
        stdin, stdout, stderr = ssh_client.exec_command(cmd_hdfs)
        exit_status = stdout.channel.recv_exit_status()
        
        if exit_status != 0:
            raise Exception(f"Error HDFS Remoto: {stderr.read().decode()}")
        
        
        cmd_spark = f"{PYTHON_BIN_AMIGO1} {RUTAS_AMIGO1['SCRIPT_NODO1']}"
        stdin, stdout, stderr = ssh_client.exec_command(cmd_spark)
        
        
        exit_status = stdout.channel.recv_exit_status()
        log_ingesta = stdout.read().decode()
        err_ingesta = stderr.read().decode()

        if exit_status != 0:
            raise Exception(f"Error Nodo1 Remoto: {err_ingesta}")

        progress_bar.progress(60, text="Ingesta finalizada.")
        st.success("✅ Datos ingeridos")
        with st.expander("Ver Logs Remotos"):
            st.code(log_ingesta)

        
        ssh_client.close()

        
        status_text.markdown("### 4. Ejecutando PLN...")

        process = subprocess.Popen(
            [RUTAS_LOCALES["PYTHON_LOCAL"], RUTAS_LOCALES["SCRIPT_NODO2"]],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        stdout_ia, stderr_ia = process.communicate()

        if process.returncode != 0:
            raise Exception(f"Error IA Local: {stderr_ia}")

        progress_bar.progress(100, text="¡Pipeline Finalizado!")
        st.success("Traducción CIE-10 completada.")
        with st.expander("Ver Logs IA"):
            st.code(stdout_ia)

        
        st.balloons()
        st.markdown(f"""
            <br>
            <a href="{URL_GRAFANA}" target="_blank">
                <button style="background-color:#FF9900;color:white;padding:15px 32px;border:none;border-radius:4px;cursor:pointer;font-size:16px;width:100%;">
                    🚀 IR AL DASHBOARD EN GRAFANA
                </button>
            </a>
        """, unsafe_allow_html=True)

    except Exception as e:
        st.error(f"❌ Error Crítico: {e}")
        if ssh_client: ssh_client.close()