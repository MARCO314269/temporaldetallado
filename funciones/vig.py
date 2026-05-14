def uvig2(cancelacion_cobertura_fc, ffin_anual, inicio_vigencia_cobertura_fc, 
          fin_vigencia_cobertura_fc, modulo, llave_poliza2):
    
    date_format = '%Y-%m-%d %H:%M:%S'
    
    def parse_date_uvig(date_str):
        if date_str is None:
            return None
        if not isinstance(date_str, str):
            return date_str
            
        date_str = date_str.strip()
        if '.' in date_str:
            date_str = date_str.split('.')[0]
        elif len(date_str.split()) > 1:
            time_part = date_str.split()[1]
            if len(time_part) > 8:
                date_str = f"{date_str.split()[0]} {time_part[:8]}"
        
        try:
            return datetime.strptime(date_str, date_format)
        except ValueError:
            return None
    
    ffin_anual = parse_date_uvig(ffin_anual)
    inicio_vigencia_cobertura_fc = parse_date_uvig(inicio_vigencia_cobertura_fc)
    fin_vigencia_cobertura_fc = parse_date_uvig(fin_vigencia_cobertura_fc)
    cancelacion_cobertura_fc = parse_date_uvig(cancelacion_cobertura_fc)
    
    modulo = modulo if modulo is not None else 0
    llave_poliza2 = llave_poliza2 if llave_poliza2 is not None else ''

    if None in (ffin_anual, inicio_vigencia_cobertura_fc, fin_vigencia_cobertura_fc):
        return Row(unidades_vigentes=0, caso=-1)

    return get_result(ffin_anual, inicio_vigencia_cobertura_fc, 
                      fin_vigencia_cobertura_fc, cancelacion_cobertura_fc, 
                      modulo, llave_poliza2)


def get_result(ffin_anual, inicio_vigencia_cobertura_fc, 
               fin_vigencia_cobertura_fc, cancelacion_cobertura_fc, 
               modulo, llave_poliza2):

    # aqui se calcula esta operacion (VIG_A)
    if inicio_vigencia_cobertura_fc > ffin_anual:
        return Row(unidades_vigentes=0, caso=1)
        
    if fin_vigencia_cobertura_fc < ffin_anual:
        return Row(unidades_vigentes=0, caso=2)
        
    if cancelacion_cobertura_fc is not None and cancelacion_cobertura_fc < ffin_anual:
        return Row(unidades_vigentes=0, caso=3)

    # --- CÓDIGO AJUSTADO (PARA EMPATAR CON EL SQL) ---
    # Al usar 'is not None', tratamos los strings vacíos como 0 (igual que SQL)
    if llave_poliza2 is not None and llave_poliza2 != '': 
        return Row(unidades_vigentes=0, caso=4)
        
    if inicio_vigencia_cobertura_fc == ffin_anual and modulo > 0:
        return Row(unidades_vigentes=0, caso=5)
    
    if inicio_vigencia_cobertura_fc == ffin_anual and modulo == 0 and cancelacion_cobertura_fc is None:
        return Row(unidades_vigentes=1, caso=6)
    
    return Row(unidades_vigentes=1, caso=7)