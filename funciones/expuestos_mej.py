import pyodbc
import pandas as pd
from datetime import datetime

# ==============================
# FUNCIONES AUXILIARES DE FECHAS
# ==============================
def max_fecha2(a, b):
    return a if a > b else b

def min_fecha(a, b, c):
    return min(a, b, c)

def min_fecha2(a, b):
    return a if a < b else b

def expuestos_py(
    fini_anual,
    ffin_anual,
    inicio_vigencia_cobertura_fc,
    fin_vigencia_cobertura_fc,
    cancelacion_cobertura_fc,
):
    """
    Traducción exacta de la función SQL dbo.EXPUESTOS.
    """
    if pd.isna(inicio_vigencia_cobertura_fc) or pd.isna(fin_vigencia_cobertura_fc):
        return 0.0

    ini = inicio_vigencia_cobertura_fc
    fin = fin_vigencia_cobertura_fc
    can = cancelacion_cobertura_fc
    
    # IMPORTANTE: Asegúrate de que las fechas sean datetime
    dias_periodo = (ffin_anual - fini_anual).days + 1

    # Condición 1: Con cancelación y traslape
    if (pd.notna(can) and ini < can and 
        ((ini <= fini_anual and fin <= ffin_anual and fin >= fini_anual and can >= fini_anual) or 
         (ini >= fini_anual and fin >= ffin_anual and ini <= ffin_anual))):
        dias = (min_fecha(ffin_anual, fin, can) - max_fecha2(fini_anual, ini)).days
        return round((dias + 0.5) / dias_periodo, 6)

    # Condición 2: Sin cancelación y traslape
    if (pd.isna(can) and ini < fin and 
        ((ini <= fini_anual and fin <= ffin_anual and fin >= fini_anual) or 
         (ini >= fini_anual and fin >= ffin_anual and ini <= ffin_anual))):
        dias = (min_fecha2(ffin_anual, fin) - max_fecha2(fini_anual, ini)).days
        return round((dias + 0.5) / dias_periodo, 6)

    # Condición 3: Dentro del periodo con cancelación
    if pd.notna(can) and (ini >= fini_anual and fin <= ffin_anual):
        dias = (min_fecha(ffin_anual, fin, can) - max_fecha2(fini_anual, ini)).days
        return round((dias + 0.0) / dias_periodo, 6)

    # Condición 4: Dentro del periodo sin cancelación
    if pd.isna(can) and (ini >= fini_anual and fin <= ffin_anual):
        dias = (min_fecha2(ffin_anual, fin) - max_fecha2(fini_anual, ini)).days
        return round((dias + 0.0) / dias_periodo, 6)

    # Condición 5: Contiene al periodo con cancelación
    if pd.notna(can) and (ini <= fini_anual and fin >= ffin_anual):
        dias = (min_fecha(ffin_anual, fin, can) - max_fecha2(fini_anual, ini)).days
        return round((dias + 0.0) / dias_periodo, 6)

    # Condición 6: Contiene al periodo sin cancelación
    if pd.isna(can) and (ini <= fini_anual and fin >= ffin_anual):
        dias = (min_fecha2(ffin_anual, fin) - max_fecha2(fini_anual, ini)).days
        return round((dias + 0.0) / dias_periodo, 6)

    # Condición 7 y 8: Casos en cero
    return 0.0

def main():
    # Fechas de periodo (Asegúrate de que coincidan con tus pruebas)
    ini_per = datetime(2024, 11, 20)
    fin_per = datetime(2025, 11, 20)

    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost\\SQLEXPRESS01;"
        "DATABASE=expuestos_prueba;"
        "Trusted_Connection=yes;"
    )

    # QUERY CORREGIDO: Usando TBL_COBERTURA
    query = """
    SELECT
        A.LLAVE_POLIZA_CERTIFICADO_CD,
        C.INICIO_VIGENCIA_COBERTURA_FC,
        C.FIN_VIGENCIA_COBERTURA_FC,
        B.FIN_VIGENCIA_POLIZA_FC,
        C.CANCELACION_COBERTURA_FC
    FROM TBL_CERTIFICADO A
    INNER JOIN TBL_COBERTURA C 
        ON A.LLAVE_POLIZA_CERTIFICADO_CD = C.LLAVE_POLIZA_CERTIFICADO_CD
    LEFT JOIN TBL_POLIZA B
        ON A.POLIZA = B.POLIZA
    """

    df = pd.read_sql(query, conn)

    # 3. Cálculo en Python aplicando la lógica ISNULL (IIF)
    df["UNIDAD_EXPUESTA"] = df.apply(
        lambda x: expuestos_py(
            ini_per,
            fin_per,
            x["INICIO_VIGENCIA_COBERTURA_FC"],
            # Lógica de respaldo: si cobertura es NULL, usa póliza
            x["FIN_VIGENCIA_COBERTURA_FC"] if pd.notna(x["FIN_VIGENCIA_COBERTURA_FC"]) 
            else x["FIN_VIGENCIA_POLIZA_FC"],
            x["CANCELACION_COBERTURA_FC"]
        ),
        axis=1,
    )

    # 4. Mostrar resultados finales
    print(df[["LLAVE_POLIZA_CERTIFICADO_CD", "UNIDAD_EXPUESTA"]])

    conn.close()

if __name__ == "__main__":
    main()