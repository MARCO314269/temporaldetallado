from datetime import datetime
from typing import Optional, Union

def obtener_caso_vigencia(
    inicio_vigencia_fc: datetime,
    ffin_anual: datetime,
    fin_vigencia_fc: datetime,
    cancelacion_fc: Optional[datetime],
    modulo: Union[str, int],
    llave_poliza2: Optional[str]
) -> int: #Esto sirve para indicar que la función devuelve un entero (el número de caso)
    """
    Recreación de la lógica de CASO de la función SQL UVIG2.
    
    Args:
        inicio_vigencia_fc: @INICIO_VIGENCIA_COBERTURA_FC
        ffin_anual: @FFIN_ANUAL
        fin_vigencia_fc: @FIN_VIGENCIA_COBERTURA_FC
        cancelacion_fc: @CANCELACION_COBERTURA_FC
        modulo: @MODULO (VARCHAR)
        llave_poliza2: @LLAVE_POLIZA2
        
    Returns:
        int: El número de caso (1-7)
    """
    # Convertir modulo a string/int para validación
    try:
        modulo_int = int(modulo)
    except (ValueError, TypeError):
        modulo_int = 0
    
    # CASO 1: @INICIO_VIGENCIA_COBERTURA_FC > @FFIN_ANUAL
    if inicio_vigencia_fc > ffin_anual:
        return 1
    
    # CASO 2: @FIN_VIGENCIA_COBERTURA_FC < @FFIN_ANUAL
    if fin_vigencia_fc < ffin_anual:
        return 2
    
    # CASO 3: @CANCELACION_COBERTURA_FC IS NOT NULL AND @CANCELACION_COBERTURA_FC < @FFIN_ANUAL
    if cancelacion_fc is not None and cancelacion_fc < ffin_anual:
        return 3
    
    # CASO 4: @LLAVE_POLIZA2 IS NOT NULL
    if llave_poliza2 is not None:
        return 4
    
    # CASO 5: @INICIO_VIGENCIA_COBERTURA_FC = @FFIN_ANUAL AND @MODULO > 0
    if inicio_vigencia_fc == ffin_anual and modulo_int > 0:
        return 5
    
    # CASO 6: @INICIO_VIGENCIA_COBERTURA_FC = @FFIN_ANUAL AND @MODULO = 0 AND @CANCELACION_COBERTURA_FC IS NULL
    if inicio_vigencia_fc == ffin_anual and modulo_int == 0 and cancelacion_fc is None:
        return 6
    
    # ELSE
    return 7
