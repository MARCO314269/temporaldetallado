import sys

import boto3
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, concat_ws, coalesce, date_format, datediff, greatest, least, lit, lower, round, row_number, sum as sum_, to_date, trim, when, year
from pyspark.sql.window import Window


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
environment = "dev"
ini_per = "2024-01-01 00:00:00"
fin_per = "2025-01-31 23:59:59"
ini_per_ANIO_ANTERIOR = "2023-01-01 11:59:59"

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")

detail_output_path = f"s3://{environment}-syp-s3-standard/autos/temporaldetallado/"
publish_output_path = f"s3://{environment}-syp-s3-analytics/autos/temporaldetallado/"
glue_database = f"{environment}-syp-glue-autos-database-temporal-detallado"
glue_table = "temporal_detallado"


def read_catalog_table(name: str):
    """Lee una tabla desde Glue Data Catalog y normaliza nombres de columnas a minusculas."""
    dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database=glue_database,
        table_name=name,
    )
    df = dynamic_frame.toDF()
    for column_name in df.columns:
        df = df.withColumnRenamed(column_name, column_name.lower())
    return df


def pick_joined_col(joined_sources, output_name: str, default_value=""):
    """Selecciona columnas de fuentes ya unidas sin romper si alguna tabla no trae el campo esperado."""
    for alias, df, candidates in joined_sources:
        for candidate in candidates:
            if df is not None and candidate in df.columns:
                return col(f"{alias}.{candidate}").alias(output_name)
    return lit(default_value).alias(output_name)


def deduplicate_latest(df, keys):
    """Conserva el registro mas reciente por llave usando carga_dt descendente."""
    window_spec = Window.partitionBy(*keys).orderBy(col("carga_dt").desc_nulls_last())
    return (
        df.withColumn("_row_num", row_number().over(window_spec))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )


def empty_s3_folder(path: str) -> None:
    """Limpia los objetos existentes en una ruta S3 para evitar archivos viejos mezclados con la nueva corrida."""
    bucket_name = path.replace("s3://", "").split("/")[0]
    prefix = "/".join(path.replace("s3://", "").split("/")[1:])
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if "Contents" in page:
            s3.delete_objects(
                Bucket=bucket_name,
                Delete={"Objects": [{"Key": obj["Key"]} for obj in page["Contents"]]},
            )


def drop_glue_table_if_exists(database_name: str, table_name: str) -> None:
    """Temporal: elimina la tabla del Data Catalog para recrear el esquema con columnas nuevas."""
    glue = boto3.client("glue")
    try:
        glue.delete_table(DatabaseName=database_name, Name=table_name)
    except glue.exceptions.EntityNotFoundException:
        return


def load_outputs(detail_df):
    """Guarda el resultado en CSV standard y publica el Parquet final en analytics/Data Catalog."""
    detail_df.write.mode("overwrite").option("header", True).option("delimiter", "^").csv(detail_output_path)

    drop_glue_table_if_exists(glue_database, glue_table)
    empty_s3_folder(publish_output_path)

    dynamic_frame = DynamicFrame.fromDF(detail_df, glueContext, "dynamic_frame_temporal_detallado")
    sink = glueContext.getSink(
        path=publish_output_path,
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        compression="snappy",
        enableUpdateCatalog=True,
        transformation_ctx="sink_temporal_detallado",
    )
    sink.setCatalogInfo(catalogDatabase=glue_database, catalogTableName=glue_table)
    sink.setFormat("parquet", useGlueParquetWriter=True)
    sink.writeFrame(dynamic_frame)


def add_general_calculation_columns(df):
    """Agrega reglas generales del temporal detallado: modulo, conducto, exposicion, caso, vigencia y prima devengada."""
    return (
        df
        .withColumn(
            "modulo_base",
            when(
                col("pol.source_cd") == "ACSEL",
                trim(coalesce(col("pol.numero_renovacion_cd").cast("string"), lit("0"))),
            ).otherwise(trim(coalesce(col("pol.modulo_solicitud_cd").cast("string"), lit("0")))),
        )
        .withColumn(
            "conducto_cobro_base",
            when(
                col("pol.source_cd") == "ACSEL",
                trim(col("cert.conducto_cobro_cd").cast("string")),
            ).otherwise(trim(col("pol.conducto_cobro_cd").cast("string"))),
        )
        .withColumn(
            "uen_base",
            when(
                col("pol.source_cd") == "ACSEL",
                trim(col("ramocert.uen_cert_ramo_cd").cast("string")),
            ).otherwise(trim(col("pol.uen_poliza_cd").cast("string"))),
        )
        .withColumn("_calc_fini_anual", to_date(lit(ini_per)))
        .withColumn("_calc_ffin_anual", to_date(lit(fin_per)))
        .withColumn("_calc_dias_periodo", datediff(col("_calc_ffin_anual"), col("_calc_fini_anual")) + lit(1))
        .withColumn("_calc_inicio_vigencia_fc", to_date(col("cert.inicio_vigencia_certificado_fc")))
        .withColumn("_calc_fin_vigencia_fc", to_date(col("pol.fin_vigencia_poliza_fc")))
        .withColumn(
            "_calc_cancelacion_fc",
            when(
                to_date(col("cert.cancelacion_certificado_fc")) <= to_date(lit("1970-01-01")),
                lit(None).cast("date"),
            ).otherwise(to_date(col("cert.cancelacion_certificado_fc"))),
        )
        .withColumn(
            "u_exp",
            when(
                col("_calc_inicio_vigencia_fc").isNull() | col("_calc_fin_vigencia_fc").isNull(),
                lit(0.0),
            ).when(
                col("_calc_cancelacion_fc").isNotNull()
                & (col("_calc_inicio_vigencia_fc") < col("_calc_cancelacion_fc"))
                & (
                    (
                        (col("_calc_inicio_vigencia_fc") <= col("_calc_fini_anual"))
                        & (col("_calc_fin_vigencia_fc") <= col("_calc_ffin_anual"))
                        & (col("_calc_fin_vigencia_fc") >= col("_calc_fini_anual"))
                        & (col("_calc_cancelacion_fc") >= col("_calc_fini_anual"))
                    )
                    | (
                        (col("_calc_inicio_vigencia_fc") >= col("_calc_fini_anual"))
                        & (col("_calc_fin_vigencia_fc") >= col("_calc_ffin_anual"))
                        & (col("_calc_inicio_vigencia_fc") <= col("_calc_ffin_anual"))
                    )
                ),
                round(
                    (datediff(least(col("_calc_ffin_anual"), col("_calc_fin_vigencia_fc"), col("_calc_cancelacion_fc")), greatest(col("_calc_fini_anual"), col("_calc_inicio_vigencia_fc"))) + lit(0.5))
                    / col("_calc_dias_periodo"),
                    6,
                ),
            ).when(
                col("_calc_cancelacion_fc").isNull()
                & (col("_calc_inicio_vigencia_fc") < col("_calc_fin_vigencia_fc"))
                & (
                    (
                        (col("_calc_inicio_vigencia_fc") <= col("_calc_fini_anual"))
                        & (col("_calc_fin_vigencia_fc") <= col("_calc_ffin_anual"))
                        & (col("_calc_fin_vigencia_fc") >= col("_calc_fini_anual"))
                    )
                    | (
                        (col("_calc_inicio_vigencia_fc") >= col("_calc_fini_anual"))
                        & (col("_calc_fin_vigencia_fc") >= col("_calc_ffin_anual"))
                        & (col("_calc_inicio_vigencia_fc") <= col("_calc_ffin_anual"))
                    )
                ),
                round(
                    (datediff(least(col("_calc_ffin_anual"), col("_calc_fin_vigencia_fc")), greatest(col("_calc_fini_anual"), col("_calc_inicio_vigencia_fc"))) + lit(0.5))
                    / col("_calc_dias_periodo"),
                    6,
                ),
            ).when(
                col("_calc_cancelacion_fc").isNotNull()
                & (col("_calc_inicio_vigencia_fc") >= col("_calc_fini_anual"))
                & (col("_calc_fin_vigencia_fc") <= col("_calc_ffin_anual")),
                round(
                    datediff(least(col("_calc_ffin_anual"), col("_calc_fin_vigencia_fc"), col("_calc_cancelacion_fc")), greatest(col("_calc_fini_anual"), col("_calc_inicio_vigencia_fc")))
                    / col("_calc_dias_periodo"),
                    6,
                ),
            ).when(
                col("_calc_cancelacion_fc").isNull()
                & (col("_calc_inicio_vigencia_fc") >= col("_calc_fini_anual"))
                & (col("_calc_fin_vigencia_fc") <= col("_calc_ffin_anual")),
                round(
                    datediff(least(col("_calc_ffin_anual"), col("_calc_fin_vigencia_fc")), greatest(col("_calc_fini_anual"), col("_calc_inicio_vigencia_fc")))
                    / col("_calc_dias_periodo"),
                    6,
                ),
            ).when(
                col("_calc_cancelacion_fc").isNotNull()
                & (col("_calc_inicio_vigencia_fc") <= col("_calc_fini_anual"))
                & (col("_calc_fin_vigencia_fc") >= col("_calc_ffin_anual")),
                round(
                    datediff(least(col("_calc_ffin_anual"), col("_calc_fin_vigencia_fc"), col("_calc_cancelacion_fc")), greatest(col("_calc_fini_anual"), col("_calc_inicio_vigencia_fc")))
                    / col("_calc_dias_periodo"),
                    6,
                ),
            ).when(
                col("_calc_cancelacion_fc").isNull()
                & (col("_calc_inicio_vigencia_fc") <= col("_calc_fini_anual"))
                & (col("_calc_fin_vigencia_fc") >= col("_calc_ffin_anual")),
                round(
                    datediff(least(col("_calc_ffin_anual"), col("_calc_fin_vigencia_fc")), greatest(col("_calc_fini_anual"), col("_calc_inicio_vigencia_fc")))
                    / col("_calc_dias_periodo"),
                    6,
                ),
            ).otherwise(lit(0.0)),
        )
        .withColumn(
            "caso",
            when(col("_calc_inicio_vigencia_fc") > col("_calc_ffin_anual"), lit(1))
            .when(col("_calc_fin_vigencia_fc") < col("_calc_ffin_anual"), lit(2))
            .when(col("_calc_cancelacion_fc").isNotNull() & (col("_calc_cancelacion_fc") < col("_calc_ffin_anual")), lit(3))
            .when((col("_calc_inicio_vigencia_fc") == col("_calc_ffin_anual")) & (col("modulo_base").cast("int") > lit(0)), lit(5))
            .when(
                (col("_calc_inicio_vigencia_fc") == col("_calc_ffin_anual"))
                & (coalesce(col("modulo_base").cast("int"), lit(0)) == lit(0))
                & col("_calc_cancelacion_fc").isNull(),
                lit(6),
            )
            .otherwise(lit(7)),
        )
        .withColumn("u_vig", when(col("caso").isin(6, 7), lit(1)).otherwise(lit(0)))
        .withColumn(
            "_calc_prima_base",
            when(coalesce(col("cert.prima_neta_inicial_cert_mnt"), lit(0.0)) != lit(0.0), col("cert.prima_neta_inicial_cert_mnt"))
            .otherwise(coalesce(col("cert.prima_neta_final_cert_mnt"), lit(0.0))),
        )
        .withColumn("_calc_diff_vigencia", datediff(col("_calc_fin_vigencia_fc"), col("_calc_inicio_vigencia_fc")))
        .withColumn(
            "primadev",
            when(col("_calc_diff_vigencia").isin(0, 365, 366), col("u_exp") * col("_calc_prima_base"))
            .when(
                col("_calc_diff_vigencia") > lit(366),
                (col("_calc_prima_base") / col("_calc_diff_vigencia")) * col("_calc_dias_periodo") * col("u_exp"),
            )
            .when(
                col("_calc_diff_vigencia") > lit(0),
                (col("_calc_prima_base") * col("u_exp")) * col("_calc_dias_periodo") / col("_calc_diff_vigencia"),
            )
            .otherwise(lit(0.0)),
        )
        .withColumn("ini_año_mes", date_format(col("cert.inicio_vigencia_certificado_fc"), "yyyyMM"))
        .withColumn("ini_año", year(col("cert.inicio_vigencia_certificado_fc")))
        .withColumn(
            "dias_vigencia_efectivos",
            when(
                col("_calc_inicio_vigencia_fc").isNull() | col("_calc_fin_vigencia_fc").isNull(),
                lit(0),
            ).otherwise(
                datediff(
                    when(
                        col("_calc_cancelacion_fc").isNotNull(),
                        least(col("_calc_fin_vigencia_fc"), col("_calc_cancelacion_fc")),
                    ).otherwise(col("_calc_fin_vigencia_fc")),
                    col("_calc_inicio_vigencia_fc"),
                )
            ),
        )
        .withColumn(
            "llave_cruce",
            when(
                col("pol.source_cd") == "SOS",
                concat_ws(
                    "-",
                    col("pol.subramo_cd"),
                    col("pol.numero_oficina_cd"),
                    col("pol.numero_poliza_cd"),
                    col("modulo_base"),
                    col("cert.certificado_num"),
                ),
            ).when(
                col("pol.source_cd") == "ACSEL",
                concat_ws(
                    "-",
                    col("pol.codigo_producto_cd"),
                    col("pol.numero_oficina_cd"),
                    col("pol.numero_poliza_cd"),
                    col("modulo_base"),
                    col("cert.certificado_num"),
                ),
            ),
        )
    )


# =============================================================================
# EXTRACT: lectura de tablas y catalogos desde Glue Data Catalog
# =============================================================================

tbl_certificado = read_catalog_table("tbl_certificado")
tbl_poliza = read_catalog_table("tbl_poliza")
tbl_movimiento_endoso = read_catalog_table("tbl_movimiento_endoso")
tbl_cobertura = read_catalog_table("tbl_cobertura")
tbl_detalle_vehiculo = read_catalog_table("tbl_detalle_vehiculo")
tbl_certificado_ramo = read_catalog_table("tbl_certificado_ramo")
tbl_descuento_poliza_cert = read_catalog_table("tbl_descuento_poliza_cert")
hc_detalle_siniestralidad = read_catalog_table("hc_detalle_siniestralidad")
cat_conducto_cobro = read_catalog_table("cat_conducto_cobro")
cat_cua = read_catalog_table("cat_cua")
cat_estado = read_catalog_table("cat_estado")
cat_marca = read_catalog_table("cat_marca")
cat_municipio = read_catalog_table("cat_municipio")
cat_sucursal = read_catalog_table("cat_sucursal")
cat_tipo_vehiculo = read_catalog_table("cat_tipo_vehiculo")
cat_tipo_vigencia = read_catalog_table("cat_tipo_vigencia")
cat_uen = read_catalog_table("cat_uen")
cat_uso = read_catalog_table("cat_uso")


# =============================================================================
# TRANSFORM: stage de descuentos, igual que los temporales iniciales del SQL
# =============================================================================

tbl_descuento_volumen = deduplicate_latest(
    tbl_descuento_poliza_cert.filter(lower(col("descuento_desc")).contains("volumen")),
    ["llave_poliza_cd", "llave_certificado_cd"],
)
tbl_descuento_experiencia = deduplicate_latest(
    tbl_descuento_poliza_cert.filter(lower(col("descuento_desc")).contains("experiencia")),
    ["llave_poliza_cd", "llave_certificado_cd"],
)
tbl_descuento_discrecional = deduplicate_latest(
    tbl_descuento_poliza_cert.filter(lower(col("descuento_desc")).contains("discrecional")),
    ["llave_poliza_cd", "llave_certificado_cd"],
)


# =============================================================================
# TRANSFORM: stage de cobertura
# =============================================================================

tbl_cobertura_detalle = deduplicate_latest(tbl_cobertura, ["llave_poliza_cd", "llave_certificado_cd"])


# =============================================================================
# TRANSFORM: stage de tablas base
# =============================================================================

tbl_certificado = tbl_certificado.filter(
    col("inicio_vigencia_certificado_fc").between(to_date(lit(ini_per_ANIO_ANTERIOR)), to_date(lit(fin_per)))
)
tbl_certificado = deduplicate_latest(tbl_certificado, ["llave_poliza_cd", "llave_certificado_cd"])

tbl_poliza = tbl_poliza.filter(
    col("inicio_vigencia_poliza_fc").between(to_date(lit(ini_per_ANIO_ANTERIOR)), to_date(lit(fin_per)))
).filter(~col("estatus_poliza_cd").isin("DES"))

tbl_certificado_ramo = deduplicate_latest(tbl_certificado_ramo, ["llave_poliza_cd", "llave_certificado_cd"])

window_movimiento = Window.partitionBy("llave_poliza_cd", "llave_certificado_cd").orderBy(col("contable_fc").desc_nulls_last())
tbl_movimiento_endoso = (
    tbl_movimiento_endoso
    .withColumn("_row_num", row_number().over(window_movimiento))
    .filter(col("_row_num") == 1)
    .drop("_row_num")
)

gastos_siniestros = (
    hc_detalle_siniestralidad
    .filter(col("tpo_mov_num").cast("string") == lit("12"))
    .filter(col("mov_fc").between(lit(ini_per), lit(fin_per)))
    .withColumn(
        "llave_cruce",
        when(
            col("source_cd") == "SOS",
            concat_ws(
                "-",
                col("subramo_cd"),
                col("ofc_emi_cd"),
                col("pol_num"),
                col("mod_cd"),
                col("cert_num_cd"),
            ),
        ).when(
            col("source_cd") == "ACSEL",
            concat_ws(
                "-",
                col("prd_cd"),
                col("ofc_emi_cd"),
                col("pol_num"),
                col("mod_cd"),
                col("cert_num_cd"),
            ),
        ),
    )
    .groupBy("llave_cruce")
    .agg(sum_(col("mov_mnt").cast("double")).alias("gastos"))
    .withColumnRenamed("llave_cruce", "llave_cruce_gastos")
)


# =============================================================================
# TRANSFORM: construccion del detalle mediante joins y reglas de negocio
# =============================================================================

detail_source = (
    tbl_certificado.alias("cert")
    .join(
        tbl_poliza.alias("pol"),
        col("cert.llave_poliza_cd") == col("pol.llave_poliza_cd"),
        "inner",
    )
    .join(
        tbl_movimiento_endoso.alias("mov"),
        (col("cert.llave_poliza_cd") == col("mov.llave_poliza_cd"))
        & (col("cert.llave_certificado_cd").cast("string") == col("mov.llave_certificado_cd").cast("string")),
        "left",
    )
    .join(
        tbl_cobertura_detalle.alias("cob"),
        (col("cert.llave_poliza_cd") == col("cob.llave_poliza_cd"))
        & (col("cert.llave_certificado_cd").cast("string") == col("cob.llave_certificado_cd").cast("string")),
        "left",
    )
    .join(
        tbl_certificado_ramo.alias("ramocert"),
        (col("cert.llave_poliza_cd") == col("ramocert.llave_poliza_cd"))
        & (col("cert.llave_certificado_cd").cast("string") == col("ramocert.llave_certificado_cd").cast("string")),
        "left",
    )
    .join(
        tbl_detalle_vehiculo.alias("veh"),
        (col("cert.llave_poliza_cd") == col("veh.llave_poliza_cd"))
        & (col("cert.llave_certificado_cd").cast("string") == col("veh.llave_certificado_cd").cast("string")),
        "left",
    )
    .join(
        tbl_descuento_volumen.alias("dctovol"),
        (col("cert.llave_poliza_cd") == col("dctovol.llave_poliza_cd"))
        & (col("cert.llave_certificado_cd").cast("string") == col("dctovol.llave_certificado_cd").cast("string")),
        "left",
    )
    .join(
        tbl_descuento_experiencia.alias("dctoexp"),
        (col("cert.llave_poliza_cd") == col("dctoexp.llave_poliza_cd"))
        & (col("cert.llave_certificado_cd").cast("string") == col("dctoexp.llave_certificado_cd").cast("string")),
        "left",
    )
    .join(
        tbl_descuento_discrecional.alias("dctodisc"),
        (col("cert.llave_poliza_cd") == col("dctodisc.llave_poliza_cd"))
        & (col("cert.llave_certificado_cd").cast("string") == col("dctodisc.llave_certificado_cd").cast("string")),
        "left",
    )
    .transform(add_general_calculation_columns)
    .join(
        gastos_siniestros.alias("gastos"),
        col("llave_cruce") == col("gastos.llave_cruce_gastos"),
        "left",
    )
)

joined_sources = {
    "cert": tbl_certificado,
    "pol": tbl_poliza,
    "mov": tbl_movimiento_endoso,
    "cob": tbl_cobertura_detalle,
    "veh": tbl_detalle_vehiculo,
    "ramocert": tbl_certificado_ramo,
    "dctovol": tbl_descuento_volumen,
    "dctoexp": tbl_descuento_experiencia,
    "dctodisc": tbl_descuento_discrecional,
    "gastos": gastos_siniestros,
}

if "agente_cd" in tbl_poliza.columns and "cve_agente" in cat_cua.columns:
    detail_source = detail_source.join(
        cat_cua.alias("cua"),
        col("pol.agente_cd").cast("string") == col("cua.cve_agente").cast("string"),
        "left",
    )
    joined_sources["cua"] = cat_cua

if "marca_vehiculo_cd" in tbl_detalle_vehiculo.columns and "marca" in cat_marca.columns:
    detail_source = detail_source.join(
        cat_marca.alias("marca"),
        col("veh.marca_vehiculo_cd").cast("string") == col("marca.marca").cast("string"),
        "left",
    )
    joined_sources["marca"] = cat_marca

if "entidad_federativa_resp_cd" in tbl_certificado.columns and "estado_cd" in cat_estado.columns:
    detail_source = detail_source.join(
        cat_estado.alias("estado"),
        col("cert.entidad_federativa_resp_cd").cast("string") == col("estado.estado_cd").cast("string"),
        "left",
    )
    joined_sources["estado"] = cat_estado

if "codigo_postal_resp_pago_cd" in tbl_certificado.columns and "codigo_postal_resp_pago_cd" in cat_municipio.columns:
    detail_source = detail_source.join(
        cat_municipio.alias("municipio"),
        col("cert.codigo_postal_resp_pago_cd").cast("string") == col("municipio.codigo_postal_resp_pago_cd").cast("string"),
        "left",
    )
    joined_sources["municipio"] = cat_municipio

if "sucursal_cd" in tbl_poliza.columns and "sucursal_cd" in cat_sucursal.columns:
    detail_source = detail_source.join(
        cat_sucursal.alias("sucursal"),
        col("pol.sucursal_cd").cast("string") == col("sucursal.sucursal_cd").cast("string"),
        "left",
    )
    joined_sources["sucursal"] = cat_sucursal

if "tipo_vehiculo_cd" in tbl_detalle_vehiculo.columns and "tipo_vehiculo_cd" in cat_tipo_vehiculo.columns:
    detail_source = detail_source.join(
        cat_tipo_vehiculo.alias("tipoveh"),
        col("veh.tipo_vehiculo_cd").cast("string") == col("tipoveh.tipo_vehiculo_cd").cast("string"),
        "left",
    )
    joined_sources["tipoveh"] = cat_tipo_vehiculo

if "tipo_vigencia_cd" in tbl_poliza.columns and "tipo_vigencia_cd" in cat_tipo_vigencia.columns:
    detail_source = detail_source.join(
        cat_tipo_vigencia.alias("tipovig"),
        col("pol.tipo_vigencia_cd").cast("string") == col("tipovig.tipo_vigencia_cd").cast("string"),
        "left",
    )
    joined_sources["tipovig"] = cat_tipo_vigencia

if "uen" in cat_uen.columns:
    detail_source = detail_source.join(
        cat_uen.alias("uen"),
        col("uen_base") == trim(col("uen.uen").cast("string")),
        "left",
    )
    joined_sources["uen"] = cat_uen

if "uso_vehiculo_cd" in tbl_detalle_vehiculo.columns and "uso_cd" in cat_uso.columns:
    detail_source = detail_source.join(
        cat_uso.alias("uso"),
        col("veh.uso_vehiculo_cd").cast("string") == col("uso.uso_cd").cast("string"),
        "left",
    )
    joined_sources["uso"] = cat_uso

if "conducto_cobro_cd" in cat_conducto_cobro.columns:
    detail_source = detail_source.join(
        cat_conducto_cobro.alias("conducto"),
        col("conducto_cobro_base") == col("conducto.conducto_cobro_cd").cast("string"),
        "left",
    )
    joined_sources["conducto"] = cat_conducto_cobro


# =============================================================================
# TRANSFORM: seleccion final de columnas del layout temporal_detallado
# =============================================================================

detail = (
    detail_source
    .select(
        concat_ws(
            "-",
            col("pol.codigo_producto_cd"),
            col("pol.numero_oficina_cd"),
            col("pol.numero_poliza_cd"),
            col("modulo_base"),
            col("cert.llave_certificado_cd"),
        ).alias("llave_poliza_certificado_cd"),
        col("pol.numero_oficina_cd").alias("oficina"),
        col("pol.numero_poliza_cd").alias("poliza"),
        col("modulo_base").alias("modulo"),
        col("cert.llave_certificado_cd").alias("certificado"),
        col("cert.asegurado_desc").alias("asegurado"),
        col("cert.rfc_asegurado_cd").alias("rfc"),
        pick_joined_col([("pol", tbl_poliza, ["contratante_desc"])], "contratante_desc"),
        col("cert.inicio_vigencia_certificado_fc").alias("inicio_vigencia_certificado_fc"),
        col("pol.fin_vigencia_poliza_fc").alias("fin_vigencia_certicado_fc"),
        col("cert.cancelacion_certificado_fc").alias("cancelacion_certificado_fc"),
        col("mov.contable_fc").alias("contable_fc"),
        col("u_exp"),
        col("u_vig"),
        col("caso"),
        col("primadev"),
        col("ini_año_mes"),
        col("ini_año"),
        when(col("pol.source_cd") == "ACSEL", col("ramocert.ramo_certificado_cd")).otherwise(col("cert.ramo_inciso_cd")).alias("ramo"),
        when(col("pol.source_cd") == "ACSEL", col("ramocert.subramo_certificado_cd")).otherwise(col("cert.subramo_hom_cd")).alias("subramo"),
        when(
            coalesce(col("cert.prima_neta_inicial_cert_mnt"), lit(0)) == lit(0),
            col("cert.prima_neta_final_cert_mnt"),
        ).otherwise(col("cert.prima_neta_inicial_cert_mnt")).alias("prima_neta"),
        coalesce(col("cert.prima_neta_cancelada_mnt"), lit(0)).alias("prima_cancelada"),
        pick_joined_col([("cert", tbl_certificado, ["forma_pago_desc"])], "forma_pago_desc"),
        pick_joined_col([("cert", tbl_certificado, ["plan_pago_meses_desc"])], "plan_pago_meses_desc"),
        pick_joined_col([("pol", tbl_poliza, ["emision_poliza_fc"])], "emision_poliza_fc"),
        pick_joined_col([("pol", tbl_poliza, ["estatus_poliza_cd"])], "estatus_poliza"),
        pick_joined_col([("pol", tbl_poliza, ["cancelacion_poliza_fc"])], "cancelacion_poliza_fc"),
        pick_joined_col([("cert", tbl_certificado, ["entidad_federativa_resp_cd"])], "estado_cd"),
        pick_joined_col([("pol", tbl_poliza, ["agente_cd"])], "agente_cd"),
        pick_joined_col([("pol", tbl_poliza, ["tipo_poliza_cd"])], "tipo_poliza"),
        pick_joined_col([("pol", tbl_poliza, ["source_cd"])], "source_cd"),
        pick_joined_col([("pol", tbl_poliza, ["sucursal_cd"])], "sucursal_cd"),
        pick_joined_col([("pol", tbl_poliza, ["tipo_vigencia_cd"])], "tipo_vigencia_cd"),
        pick_joined_col([("veh", tbl_detalle_vehiculo, ["tipo_vehiculo_desc"])], "tipo_vehiculo_desc"),
        pick_joined_col([("veh", tbl_detalle_vehiculo, ["uso_vehiculo_cd"])], "uso_cd"),
        pick_joined_col([("veh", tbl_detalle_vehiculo, ["uso_vehiculo_desc"]), ("uso", joined_sources.get("uso"), ["uso", "uso_desc"])], "uso_desc"),
        pick_joined_col([("veh", tbl_detalle_vehiculo, ["carroceria_vehiculo_desc"])], "carroceria_vehiculo_desc"),
        pick_joined_col([("veh", tbl_detalle_vehiculo, ["estilo_auto_cd", "estilo_vehiculo_cd", "sbg"])], "sbg"),
        pick_joined_col([("veh", tbl_detalle_vehiculo, ["grupo_estadistico_sbg_cd"])], "gpo_est"),
        pick_joined_col([("veh", tbl_detalle_vehiculo, ["ocupantes_vehiculo_num", "num_ocupantes"])], "num_ocupantes"),
        pick_joined_col([("veh", tbl_detalle_vehiculo, ["modelo_vehiculo_num", "modelo"])], "modelo"),
        pick_joined_col([("veh", tbl_detalle_vehiculo, ["marca_vehiculo_cd"])], "marca"),
        pick_joined_col([("veh", tbl_detalle_vehiculo, ["marca_vehiculo_cd"])], "marca_vehiculo_cd"),
        pick_joined_col([("veh", tbl_detalle_vehiculo, ["marca_vehiculo_corta_desc", "tipo_reduc", "tipo_reduccion_cd"])], "tipo_reduc"),
        pick_joined_col([("veh", tbl_detalle_vehiculo, ["tonelaje_vehiculo_desc", "tonelaje"])], "tonelaje"),
        pick_joined_col([("veh", tbl_detalle_vehiculo, ["tipo_valor_vehiculo_desc"])], "tipo_valor_vehiculo_desc"),
        pick_joined_col([("veh", tbl_detalle_vehiculo, ["valor_nuevo_mnt", "valor_nuevo"])], "valor_nuevo"),
        pick_joined_col([("veh", tbl_detalle_vehiculo, ["valor_comercial_mnt", "valor_comercial"])], "valor_comercial"),
        pick_joined_col([("veh", tbl_detalle_vehiculo, ["valor_usado_mnt", "valor_usado"])], "valor_usado"),
        pick_joined_col([("veh", tbl_detalle_vehiculo, ["serie_vehiculo_num", "serie"])], "serie"),
        col("uen_base").alias("uen_cd"),
        col("conducto_cobro_base").alias("conducto_cobro_cd"),
        when(col("pol.source_cd") == "ACSEL", trim(col("ramocert.paquete_cd"))).otherwise(trim(col("cert.paquete_cd"))).alias("paquete_cd"),
        when(col("pol.source_cd") == "ACSEL", trim(col("ramocert.paquete_hom_desc"))).otherwise(trim(col("cert.paquete_hom_desc"))).alias("paquete_hom_desc"),
        when(col("pol.source_cd") == "ACSEL", trim(col("ramocert.paquete_desc"))).otherwise(trim(col("cert.paquete_desc"))).alias("paquete_desc"),
        pick_joined_col([("dctovol", tbl_descuento_volumen, ["descuento_pct"])], "dcto_volumen"),
        pick_joined_col([("dctoexp", tbl_descuento_experiencia, ["descuento_pct"])], "dcto_experiencia"),
        pick_joined_col([("dctodisc", tbl_descuento_discrecional, ["descuento_pct"])], "dcto_discrecional"),
        col("dias_vigencia_efectivos"),
        col("primadev").alias("primadev_total"),
        col("llave_cruce"),
        pick_joined_col([("municipio", joined_sources.get("municipio"), ["poblacion_resp_pago_desc"]), ("cert", tbl_certificado, ["poblacion_resp_pago_desc"])], "municipio_asegurado"),
        pick_joined_col([("cert", tbl_certificado, ["codigo_postal_resp_pago_cd", "cod_post_asegurado"])], "cod_post_asegurado"),
        pick_joined_col([("conducto", joined_sources.get("conducto"), ["conducto_cobro", "conducto_cobro_desc"])], "conducto_cobro"),
        pick_joined_col([("cua", joined_sources.get("cua"), ["agente"]), ("pol", tbl_poliza, ["nombre_agente_desc"])], "agente"),
        pick_joined_col([("cua", joined_sources.get("cua"), ["cve_prom", "cve_promotor"])], "cve_promotor"),
        pick_joined_col([("cua", joined_sources.get("cua"), ["nombre_promotor", "promotor"])], "promotor"),
        pick_joined_col([("cua", joined_sources.get("cua"), ["region_lp"])], "region_lp"),
        pick_joined_col([("cua", joined_sources.get("cua"), ["territorial_lp"])], "territorial_lp"),
        pick_joined_col([("estado", joined_sources.get("estado"), ["estado"])], "estado"),
        pick_joined_col([("marca", joined_sources.get("marca"), ["marca_desc"])], "marca_desc"),
        pick_joined_col([("sucursal", joined_sources.get("sucursal"), ["regional_lp", "regional_bs"])], "regional_lp"),
        pick_joined_col([("sucursal", joined_sources.get("sucursal"), ["sucursal"])], "sucursal"),
        pick_joined_col([("sucursal", joined_sources.get("sucursal"), ["territorial_bs"])], "territorial_bs"),
        pick_joined_col([("tipoveh", joined_sources.get("tipoveh"), ["tipo_vehiculo"])], "tipo_vehiculo"),
        pick_joined_col([("tipovig", joined_sources.get("tipovig"), ["tipo_vigencia"])], "tipo_vigencia"),
        pick_joined_col([("uen", joined_sources.get("uen"), ["linea_agrupacion", "linea"])], "linea"),
        pick_joined_col([("uen", joined_sources.get("uen"), ["linea_agrup"])], "linea_agrup"),
        pick_joined_col([("uen", joined_sources.get("uen"), ["descripcion", "sublinea"])], "sublinea"),
        pick_joined_col([("uso", joined_sources.get("uso"), ["uso"])], "uso"),
        coalesce(col("gastos.gastos"), lit(0)).alias("gastos"),
        pick_joined_col([("cob", tbl_cobertura_detalle, ["cobertura_cd"])], "cobertura_cd"),
        pick_joined_col([("cob", tbl_cobertura_detalle, ["cobertura_desc"])], "cobertura_desc"),
        pick_joined_col([("cob", tbl_cobertura_detalle, ["inicio_vigencia_cobertura_fc"])], "inicio_vigencia_cobertura_fc"),
        pick_joined_col([("cob", tbl_cobertura_detalle, ["fin_vigencia_cobertura_fc"])], "fin_vigencia_cobertura_fc"),
        pick_joined_col([("cob", tbl_cobertura_detalle, ["cancelacion_cobertura_fc"])], "cancelacion_cobertura_fc"),
        pick_joined_col([("cob", tbl_cobertura_detalle, ["prima_neta_inicial_mnt"])], "prima_neta_inicial_mnt"),
        pick_joined_col([("cob", tbl_cobertura_detalle, ["prima_neta_final_mnt"])], "prima_neta_final_mnt"),
    )
    .filter(col("llave_poliza_certificado_cd").isNotNull())
    .dropDuplicates(["llave_poliza_certificado_cd"])
)


# =============================================================================
# LOAD: escritura en S3 y publicacion en Glue Data Catalog
# =============================================================================

load_outputs(detail)

job.commit()
