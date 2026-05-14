import pyodbc
import pandas as pd
import math
from datetime import datetime

# --- Lógica de Prima Devengada (PDEV) ---

def pdev_sql_exacto(valor_expuesto, ini_per, fin_per, inicio_vig, fin_vig, prima_ini, prima_fin):
    """
    Replica la lógica de la función [dbo].[PDEV] de SQL Server.
    """
    # 1. Manejo de nulos y tipos
    exp = float(valor_expuesto) if valor_expuesto is not None else 0.0
    p_ini = float(prima_ini) if prima_ini is not None else 0.0
    p_fin = float(prima_fin) if prima_fin is not None else 0.0
    
    # 2. Selección de Prima Base (Prioridad a la Inicial si no es 0.0)
    # Equivalente al CASE de SQL: WHEN @PRIMA_NETA_INICIAL_MNT != 0.0
    epsilon = 1e-9
    base_val = p_ini if abs(p_ini) > epsilon else p_fin

    # 3. Cálculos de días
    # DATEDIFF(day, inicio, fin) en SQL es simplemente resta de fechas en Python
    diff_vigencia = (fin_vig - inicio_vig).days
    
    # DATEDIFF(day, @FINI_ANUAL, @FFIN_ANUAL) + 1
    diff_anual = (fin_per - ini_per).days + 1

    # 4. Escenarios de la función SQL (Condiciones 1 a 6)
    
    # Condición 3 y 6: Pólizas anuales (0, 365, 366 días)
    if diff_vigencia in [0, 365, 366]:
        return exp * base_val
    
    # Condición 1 y 4: Pólizas multianuales (> 366)
    elif diff_vigencia > 366:
        # (@PRIMA / diff_vigencia) * (diff_anual) * @UNIDAD_EXPUESTA
        return (base_val / diff_vigencia) * diff_anual * exp
    
    # Condición 2 y 5: Pólizas menores a un año (< 365 y > 0)
    elif diff_vigencia > 0:
        # (@PRIMA * @UNIDAD_EXPUESTA) * (diff_anual) / (diff_vigencia)
        return (base_val * exp) * diff_anual / diff_vigencia

    return 0.0

# --- Conexión y Carga de Datos ---

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost\\SQLEXPRESS01;"
    "DATABASE=expuestos_prueba;"
    "Trusted_Connection=yes;"
)

# Ajustamos el query para traer lo necesario para PDEV
# Nota: Usamos la estructura de JOIN que te funcionó en SQL
query = """
DECLARE @INI_PER DATETIME2 = '2024-11-20'
DECLARE @FIN_PER DATETIME2 = '2025-11-20'

SELECT 
    A.LLAVE_POLIZA_CERTIFICADO_CD,
    C.INICIO_VIGENCIA_COBERTURA_FC AS INICIO_COB,
    ISNULL(C.FIN_VIGENCIA_COBERTURA_FC, B.FIN_VIGENCIA_POLIZA_FC) AS FIN_COB,
    C.PRIMA_NETA_INICIAL_MNT AS P_INI,
    C.PRIMA_NETA_FINAL_MNT AS P_FIN,
    
    -- Resultado previo de Expuestos (paso necesario para PDEV)
    dbo.EXPUESTOS(@INI_PER, @FIN_PER, C.INICIO_VIGENCIA_COBERTURA_FC, 
                  ISNULL(C.FIN_VIGENCIA_COBERTURA_FC, B.FIN_VIGENCIA_POLIZA_FC), 
                  C.CANCELACION_COBERTURA_FC) AS EXP_SQL,
                  
    -- Resultado de PDEV en SQL
    dbo.PDEV(
        dbo.EXPUESTOS(@INI_PER, @FIN_PER, C.INICIO_VIGENCIA_COBERTURA_FC, 
                      ISNULL(C.FIN_VIGENCIA_COBERTURA_FC, B.FIN_VIGENCIA_POLIZA_FC), 
                      C.CANCELACION_COBERTURA_FC),
        @INI_PER, @FIN_PER, C.INICIO_VIGENCIA_COBERTURA_FC,
        ISNULL(C.FIN_VIGENCIA_COBERTURA_FC, B.FIN_VIGENCIA_POLIZA_FC),
        C.PRIMA_NETA_INICIAL_MNT, C.PRIMA_NETA_FINAL_MNT
    ) AS PDEV_SQL

FROM TBL_CERTIFICADO A
INNER JOIN TBL_COBERTURA C ON A.LLAVE_POLIZA_CERTIFICADO_CD = C.LLAVE_POLIZA_CERTIFICADO_CD
LEFT JOIN TBL_POLIZA B ON A.POLIZA = B.POLIZA
"""

df = pd.read_sql(query, conn)

# --- Comparación ---

ini_per = datetime(2024, 11, 20)
fin_per = datetime(2025, 11, 20)

# Aplicamos la función de Python fila por fila
df["PDEV_PY"] = df.apply(
    lambda x: pdev_sql_exacto(
        x["EXP_SQL"], # Usamos el expuesto calculado
        ini_per,
        fin_per,
        x["INICIO_COB"],
        x["FIN_COB"],
        x["P_INI"],
        x["P_FIN"]
    ),
    axis=1
)

# Diferencia entre SQL y Python
df["DIFF"] = df["PDEV_SQL"].astype(float) - df["PDEV_PY"]

# Redondeamos para facilitar la vista
df["PDEV_PY"] = df["PDEV_PY"].round(4)

print(df[['LLAVE_POLIZA_CERTIFICADO_CD', 'PDEV_SQL', 'PDEV_PY', 'DIFF']])