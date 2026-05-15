import sys
import re
from copy import deepcopy
from typing import Callable, Dict, Iterable, List, Optional

import boto3
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, max as spark_max, to_timestamp, trim
from pyspark.sql.types import StringType


DEFAULT_DATE = "1970-01-01 00:00:00"


def parse_args() -> Dict[str, str]:
    required = ["JOB_NAME", "ENV"]
    optional = {
        "DATABASE": None,
        "TABLES": "all",
        "INCLUDE_DERIVED": "true",
        "PROCESS_DATA": "true",
        "UPDATE_CATALOG_LOCATION": "false",
        "RAW_BUCKET": None,
        "STANDARD_BUCKET": None,
    }

    present = [arg[2:].split("=", 1)[0] for arg in sys.argv if arg.startswith("--")]
    names = required + [name for name in optional if name in present]
    args = getResolvedOptions(sys.argv, names)

    for key, value in optional.items():
        args.setdefault(key, value)

    env = args["ENV"]
    args["DATABASE"] = args["DATABASE"] or f"{env}-syp-glue-autos-database-temporal-detallado"
    args["RAW_BUCKET"] = args["RAW_BUCKET"] or f"{env}-syp-s3-raw"
    args["STANDARD_BUCKET"] = args["STANDARD_BUCKET"] or f"{env}-syp-s3-standard"
    return args


args = parse_args()
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")

s3 = boto3.resource("s3")
glue = boto3.client("glue")

DATABASE = args["DATABASE"]
RAW_BASE = f"s3://{args['RAW_BUCKET']}/autos/mineria/temporaldetallado"
STANDARD_BASE = f"s3://{args['STANDARD_BUCKET']}/autos/mineria/temporaldetallado"


def normalize_name(name: str) -> str:
    normalized = name.strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized or "columna"


def lower_columns(df: DataFrame) -> DataFrame:
    used_names = set()
    for column_name in df.columns:
        normalized = normalize_name(column_name)
        base_name = normalized
        suffix = 2
        while normalized in used_names:
            normalized = f"{base_name}_{suffix}"
            suffix += 1
        used_names.add(normalized)
        if column_name != normalized:
            df = df.withColumnRenamed(column_name, normalized)
    return df


def has_col(df: DataFrame, column_name: str) -> bool:
    return column_name in df.columns


def trim_string_columns(df: DataFrame, columns: Optional[Iterable[str]] = None) -> DataFrame:
    if columns:
        selected = list(columns)
    else:
        selected = [
            field.name
            for field in df.schema.fields
            if isinstance(field.dataType, StringType)
        ]
    for column_name in selected:
        if has_col(df, column_name):
            df = df.withColumn(column_name, trim(col(column_name).cast(StringType())))
    return df


def cast_timestamps(df: DataFrame, columns: Iterable[str]) -> DataFrame:
    for column_name in columns:
        if has_col(df, column_name):
            df = df.withColumn(column_name, to_timestamp(col(column_name), "yyyy-MM-dd HH:mm:ss"))
    return df


def fill_existing(df: DataFrame, values: Dict[str, object]) -> DataFrame:
    existing = {key: value for key, value in values.items() if has_col(df, key)}
    return df.fillna(existing) if existing else df


def cast_existing(df: DataFrame, columns: Iterable[str], data_type: str) -> DataFrame:
    for column_name in columns:
        if has_col(df, column_name):
            df = df.withColumn(column_name, col(column_name).cast(data_type))
    return df


def clear_s3_path(path: str) -> None:
    bucket_name = path.replace("s3://", "").split("/", 1)[0]
    prefix = path.replace("s3://", "").split("/", 1)[1].rstrip("/") + "/"
    bucket = s3.Bucket(bucket_name)
    print(f"Limpiando S3: s3://{bucket_name}/{prefix}")
    bucket.objects.filter(Prefix=prefix).delete()


def read_source(config: Dict[str, object]) -> DataFrame:
    raw_path = config.get("raw_path")
    if raw_path:
        print(f"Leyendo {config['target_table']} desde ruta raw {raw_path}")
        return read_raw_path(config, raw_path)

    raise RuntimeError(f"No se encontro ruta raw para {config['target_table']}")


def read_raw_path(config: Dict[str, object], raw_path: str) -> DataFrame:
    if config["format"] == "csv":
        delimiter = config.get("delimiter") or infer_delimiter(raw_path)
        return lower_columns(
            spark.read.option("header", True)
            .option("sep", delimiter)
            .option("mode", "PERMISSIVE")
            .csv(raw_path)
        )
    return lower_columns(spark.read.parquet(raw_path))


def infer_delimiter(raw_path: str) -> str:
    candidates = ["\t", "~", "|", ","]
    first_line_rows = spark.read.text(raw_path).limit(1).collect()
    if not first_line_rows:
        print(f"No se pudo inferir delimitador para {raw_path}; usando tabulador.")
        return "\t"

    first_line = first_line_rows[0]["value"]
    delimiter = max(candidates, key=lambda candidate: first_line.count(candidate))
    printable = "\\t" if delimiter == "\t" else delimiter
    print(f"Delimitador inferido para {raw_path}: {printable}")
    return delimiter


def read_standard_parquet(table_name: str) -> DataFrame:
    path = f"{STANDARD_BASE}/{table_name}/"
    print(f"Leyendo {table_name} desde standard path {path}")
    return lower_columns(spark.read.parquet(path))


def write_catalog_table(df: DataFrame, config: Dict[str, object]) -> None:
    target_table = config["target_table"]
    target_path = config["target_path"]
    target_format = config["format"]
    partition_keys = config.get("partition_keys", [])
    clear_s3_path(target_path)

    write_df = df
    frame = DynamicFrame.fromDF(write_df, glueContext, f"frame_{target_table}")
    sink = glueContext.getSink(
        path=target_path,
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=partition_keys,
        enableUpdateCatalog=True,
        transformation_ctx=f"sink_{target_table}",
    )
    sink.setCatalogInfo(catalogDatabase=DATABASE, catalogTableName=target_table)

    if target_format == "csv":
        sink.setFormat("csv", separator="\t", withHeader=True)
    else:
        sink.setFormat("glueparquet", compression="snappy")

    sink.writeFrame(frame)
    print(f"Tabla escrita: {target_table} -> {target_path}")


def sanitize_table_input(table: Dict[str, object]) -> Dict[str, object]:
    allowed_keys = {
        "Name",
        "Description",
        "Owner",
        "LastAccessTime",
        "LastAnalyzedTime",
        "Retention",
        "StorageDescriptor",
        "PartitionKeys",
        "ViewOriginalText",
        "ViewExpandedText",
        "TableType",
        "Parameters",
        "TargetTable",
    }
    return {key: deepcopy(value) for key, value in table.items() if key in allowed_keys}


def apply_catalog_csv_format(table_input: Dict[str, object]) -> None:
    storage = table_input["StorageDescriptor"]
    storage["InputFormat"] = "org.apache.hadoop.mapred.TextInputFormat"
    storage["OutputFormat"] = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    storage["SerdeInfo"] = {
        "SerializationLibrary": "org.apache.hadoop.hive.serde2.OpenCSVSerde",
        "Parameters": {
            "separatorChar": "\t",
            "quoteChar": '"',
            "escapeChar": "\\",
        },
    }
    table_input.setdefault("Parameters", {})
    table_input["Parameters"].update(
        {
            "classification": "csv",
            "skip.header.line.count": "1",
            "UPDATED_BY": "raw_a_standard_temporal_detallado_glue.py",
        }
    )


def apply_catalog_parquet_format(table_input: Dict[str, object]) -> None:
    storage = table_input["StorageDescriptor"]
    storage["InputFormat"] = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    storage["OutputFormat"] = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    storage["SerdeInfo"] = {
        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
        "Parameters": storage.get("SerdeInfo", {}).get("Parameters", {}),
    }
    table_input.setdefault("Parameters", {})
    table_input["Parameters"].update(
        {
            "classification": "parquet",
            "UPDATED_BY": "raw_a_standard_temporal_detallado_glue.py",
        }
    )


def update_catalog_location(config: Dict[str, object]) -> None:
    target_table = config["target_table"]
    target_path = config["target_path"]
    target_format = config["format"]

    table = glue.get_table(DatabaseName=DATABASE, Name=target_table)["Table"]
    table_input = sanitize_table_input(table)
    table_input["StorageDescriptor"]["Location"] = target_path

    if target_format == "csv":
        apply_catalog_csv_format(table_input)
    else:
        apply_catalog_parquet_format(table_input)

    glue.update_table(DatabaseName=DATABASE, TableInput=table_input)
    print(f"Catalog actualizado: {DATABASE}.{target_table} -> {target_path}")

    if config.get("partition_keys"):
        print(
            f"Nota: {target_table} tiene particiones. Si Athena no las ve, ejecuta: "
            f"MSCK REPAIR TABLE \"{DATABASE}\".\"{target_table}\";"
        )


def maybe_update_catalog_location(config: Dict[str, object]) -> None:
    if str(args["UPDATE_CATALOG_LOCATION"]).lower() != "true":
        return
    try:
        update_catalog_location(config)
    except glue.exceptions.EntityNotFoundException:
        print(
            f"No se actualizo catalogo porque no existe la tabla: "
            f"{DATABASE}.{config['target_table']}"
        )


def transform_simple_catalog(df: DataFrame) -> DataFrame:
    return trim_string_columns(lower_columns(df)).dropDuplicates()


def transform_tbl_poliza(df: DataFrame) -> DataFrame:
    df = lower_columns(df)
    df = fill_existing(
        df,
        {
            "inicio_vigencia_poliza_fc": DEFAULT_DATE,
            "fin_vigencia_poliza_fc": DEFAULT_DATE,
            "emision_poliza_fc": DEFAULT_DATE,
            "cancelacion_poliza_fc": DEFAULT_DATE,
            "carga_dt": DEFAULT_DATE,
            "prima_neta_inicial_mnt": 0.0,
            "prima_neta_final_mnt": 0.0,
        },
    )
    df = cast_timestamps(
        df,
        [
            "inicio_vigencia_poliza_fc",
            "fin_vigencia_poliza_fc",
            "emision_poliza_fc",
            "cancelacion_poliza_fc",
            "carga_dt",
        ],
    )
    return trim_string_columns(df).dropDuplicates()


def transform_tbl_certificado(df: DataFrame) -> DataFrame:
    df = lower_columns(df)
    df = fill_existing(
        df,
        {
            "inicio_vigencia_certificado_fc": DEFAULT_DATE,
            "estatus_certificado_fc": DEFAULT_DATE,
            "cancelacion_certificado_fc": DEFAULT_DATE,
            "carga_dt": DEFAULT_DATE,
            "comision_poliza_cert_pct": 0.0,
            "prima_neta_inicial_cert_mnt": 0.0,
            "prima_neta_final_cert_mnt": 0.0,
            "prima_neta_cancelada_mnt": 0.0,
        },
    )
    df = cast_timestamps(
        df,
        [
            "inicio_vigencia_certificado_fc",
            "estatus_certificado_fc",
            "cancelacion_certificado_fc",
            "carga_dt",
        ],
    )
    return trim_string_columns(df).dropDuplicates()


def transform_tbl_certificado_ramo(df: DataFrame) -> DataFrame:
    df = lower_columns(df)
    df = fill_existing(df, {"carga_dt": DEFAULT_DATE})
    df = cast_timestamps(df, ["carga_dt"])
    df = cast_existing(
        df,
        [
            "llave_poliza_cd",
            "llave_certificado_cd",
            "llave_ramo_cd",
            "ramo_certificado_cd",
            "subramo_certificado_cd",
            "plan_cd",
            "linea_est_hom_desc",
            "sublinea_hom_desc",
            "uen_cert_ramo_cd",
            "paquete_hom_desc",
            "paquete_cd",
            "paquete_desc",
            "month",
            "year",
        ],
        "string",
    )
    return trim_string_columns(df).dropDuplicates()


def transform_tbl_detalle_vehiculo(df: DataFrame) -> DataFrame:
    df = lower_columns(df)
    df = fill_existing(
        df,
        {
            "carga_dt": DEFAULT_DATE,
            "valor_nuevo_mnt": 0.0,
            "valor_usado_mnt": 0.0,
            "valor_comercial_mnt": 0.0,
        },
    )
    df = cast_existing(
        df,
        ["valor_nuevo_mnt", "valor_usado_mnt", "valor_comercial_mnt"],
        "decimal(15,2)",
    )
    df = cast_timestamps(df, ["carga_dt"])
    return trim_string_columns(df).dropDuplicates()


def transform_tbl_movimiento_endoso(df: DataFrame) -> DataFrame:
    df = lower_columns(df)
    df = fill_existing(df, {"contable_fc": DEFAULT_DATE})
    df = cast_timestamps(df, ["contable_fc"])
    required = ["llave_poliza_cd", "llave_certificado_cd", "contable_fc"]
    missing = [column_name for column_name in required if not has_col(df, column_name)]
    if missing:
        raise RuntimeError(f"tbl_movimiento_endoso no tiene columnas requeridas: {missing}")
    return (
        df.groupBy("llave_poliza_cd", "llave_certificado_cd")
        .agg(spark_max("contable_fc").alias("contable_fc"))
        .dropDuplicates()
    )


def transform_tbl_descuento_poliza_cert(df: DataFrame) -> DataFrame:
    df = lower_columns(df)
    df = fill_existing(df, {"registro_dt": DEFAULT_DATE, "descuento_pct": 0.0})
    df = cast_timestamps(df, ["registro_dt"])
    return trim_string_columns(df).dropDuplicates()


def transform_tbl_cobertura(df: DataFrame) -> DataFrame:
    df = lower_columns(df)
    df = fill_existing(
        df,
        {
            "carga_dt": DEFAULT_DATE,
            "inicio_vigencia_cobertura_fc": DEFAULT_DATE,
            "fin_vigencia_cobertura_fc": DEFAULT_DATE,
            "cancelacion_cobertura_fc": DEFAULT_DATE,
            "prima_neta_inicial_mnt": 0.0,
            "prima_neta_final_mnt": 0.0,
            "deducible_cobertura_mnt": 0.0,
            "suma_asegurada_mnt": 0.0,
        },
    )
    df = cast_existing(
        df,
        [
            "prima_neta_inicial_mnt",
            "prima_neta_final_mnt",
            "deducible_cobertura_mnt",
            "suma_asegurada_mnt",
        ],
        "double",
    )
    df = cast_timestamps(
        df,
        [
            "carga_dt",
            "inicio_vigencia_cobertura_fc",
            "fin_vigencia_cobertura_fc",
            "cancelacion_cobertura_fc",
        ],
    )
    return trim_string_columns(df).dropDuplicates()


def transform_hc_detalle_siniestralidad(df: DataFrame) -> DataFrame:
    df = lower_columns(df)
    df = fill_existing(
        df,
        {
            "ini_vig_pol_fc": DEFAULT_DATE,
            "fin_vig_pol_fc": DEFAULT_DATE,
            "can_pol_fc": DEFAULT_DATE,
            "apl_pol_fc": DEFAULT_DATE,
            "est_sin_fc": DEFAULT_DATE,
            "ocu_fc": DEFAULT_DATE,
            "reg_fc": DEFAULT_DATE,
            "rep_fc": DEFAULT_DATE,
            "mov_fc": DEFAULT_DATE,
            "ing_mesa_fc": DEFAULT_DATE,
            "ing_fact_fc": DEFAULT_DATE,
            "inf_pago_fc": DEFAULT_DATE,
            "inf_cont_fc": DEFAULT_DATE,
            "ing_teso_fc": DEFAULT_DATE,
            "ing_pag_prov_fc": DEFAULT_DATE,
            "carga_dt": DEFAULT_DATE,
            "sum_aseg_mnt": "0.0",
            "mov_mnt": "0.0",
        },
    )
    df = cast_existing(df, ["mod_cd", "anio_num", "cert_num_cd", "sum_aseg_mnt", "mov_mnt"], "string")
    return trim_string_columns(df).dropDuplicates()


def build_tbl_renovaciones_certificado() -> DataFrame:
    for table_name in ["tbl_certificado", "tbl_poliza", "tbl_detalle_vehiculo"]:
        df = read_standard_parquet(table_name)
        df.createOrReplaceTempView(table_name)

    query = """
        SELECT
            veh.serie_vehiculo_num,
            pol.llave_poliza_cd,
            pol.fin_vigencia_poliza_fc,
            pol.cancelacion_poliza_fc,
            b.llave_poliza_cd AS llave_poliza_cd2,
            b.inicio_vigencia_certificado_fc,
            b.cancelacion_certificado_fc,
            b.serie_vehiculo_num2
        FROM tbl_detalle_vehiculo AS veh
        LEFT JOIN (
            SELECT llave_poliza_cd, fin_vigencia_poliza_fc, cancelacion_poliza_fc, numero_oficina_cd
            FROM tbl_poliza
        ) AS pol
            ON pol.llave_poliza_cd = veh.llave_poliza_cd
        LEFT JOIN (
            SELECT
                veh2.serie_vehiculo_num AS serie_vehiculo_num2,
                cert2.llave_poliza_cd AS llave_poliza_cd,
                cert2.inicio_vigencia_certificado_fc,
                cert2.cancelacion_certificado_fc
            FROM tbl_detalle_vehiculo AS veh2
            LEFT JOIN (
                SELECT llave_poliza_cd, llave_certificado_cd, inicio_vigencia_certificado_fc, cancelacion_certificado_fc
                FROM tbl_certificado
            ) AS cert2
                ON cert2.llave_poliza_cd = veh2.llave_poliza_cd
               AND cert2.llave_certificado_cd = veh2.llave_certificado_cd
            WHERE cert2.cancelacion_certificado_fc = TIMESTAMP '1970-01-01 00:00:00'
        ) AS b
            ON b.serie_vehiculo_num2 = veh.serie_vehiculo_num
           AND b.inicio_vigencia_certificado_fc = TIMESTAMP '2023-08-31 00:00:00'
        WHERE b.serie_vehiculo_num2 IS NOT NULL
          AND pol.numero_oficina_cd NOT IN ('54D')
          AND pol.cancelacion_poliza_fc = TIMESTAMP '1970-01-01 00:00:00'
          AND b.llave_poliza_cd LIKE '%0'
        GROUP BY
            veh.serie_vehiculo_num,
            pol.llave_poliza_cd,
            pol.fin_vigencia_poliza_fc,
            pol.cancelacion_poliza_fc,
            b.llave_poliza_cd,
            b.inicio_vigencia_certificado_fc,
            b.cancelacion_certificado_fc,
            b.serie_vehiculo_num2
    """
    return spark.sql(query)


def build_tbl_renovaciones_cobertura() -> DataFrame:
    for table_name in ["tbl_detalle_vehiculo", "tbl_cobertura"]:
        df = read_standard_parquet(table_name)
        df.createOrReplaceTempView(table_name)

    query = """
        SELECT
            veh.serie_vehiculo_num,
            cob.llave_poliza_cd,
            cob.fin_vigencia_cobertura_fc,
            cob.cancelacion_cobertura_fc,
            b.llave_poliza_cd AS llave_poliza_cd2,
            b.inicio_vigencia_cobertura_fc,
            b.cancelacion_cobertura_fc AS cancelacion_cobertura_fc2,
            b.serie_vehiculo_num2
        FROM tbl_detalle_vehiculo AS veh
        LEFT JOIN (
            SELECT llave_poliza_cd, llave_certificado_cd, fin_vigencia_cobertura_fc, cancelacion_cobertura_fc
            FROM tbl_cobertura
        ) AS cob
            ON cob.llave_poliza_cd = veh.llave_poliza_cd
           AND cob.llave_certificado_cd = veh.llave_certificado_cd
        LEFT JOIN (
            SELECT
                veh2.serie_vehiculo_num AS serie_vehiculo_num2,
                cob2.llave_poliza_cd AS llave_poliza_cd,
                cob2.inicio_vigencia_cobertura_fc,
                cob2.cancelacion_cobertura_fc
            FROM tbl_detalle_vehiculo AS veh2
            LEFT JOIN (
                SELECT llave_poliza_cd, llave_certificado_cd, inicio_vigencia_cobertura_fc, cancelacion_cobertura_fc
                FROM tbl_cobertura
            ) AS cob2
                ON cob2.llave_poliza_cd = veh2.llave_poliza_cd
               AND cob2.llave_certificado_cd = veh2.llave_certificado_cd
            WHERE cob2.cancelacion_cobertura_fc = TIMESTAMP '1970-01-01 00:00:00'
        ) AS b
            ON b.serie_vehiculo_num2 = veh.serie_vehiculo_num
           AND b.inicio_vigencia_cobertura_fc = cob.fin_vigencia_cobertura_fc
        WHERE b.inicio_vigencia_cobertura_fc = TIMESTAMP '2022-07-31 00:00:00'
          AND b.serie_vehiculo_num2 IS NOT NULL
          AND cob.cancelacion_cobertura_fc = TIMESTAMP '1970-01-01 00:00:00'
          AND lower(cob.llave_poliza_cd) NOT LIKE '%-54d-%'
          AND b.llave_poliza_cd LIKE '%0'
        GROUP BY
            veh.serie_vehiculo_num,
            cob.llave_poliza_cd,
            cob.fin_vigencia_cobertura_fc,
            cob.cancelacion_cobertura_fc,
            b.llave_poliza_cd,
            b.inicio_vigencia_cobertura_fc,
            b.cancelacion_cobertura_fc,
            b.serie_vehiculo_num2
    """
    return spark.sql(query)


Transform = Callable[[DataFrame], DataFrame]


def cat_config(table_name: str) -> Dict[str, object]:
    return {
        "target_table": table_name,
        "raw_path": f"{RAW_BASE}/{table_name}",
        "target_path": f"{STANDARD_BASE}/{table_name}/",
        "format": "csv",
        "transform": transform_simple_catalog,
        "partition_keys": [],
    }


TABLE_CONFIGS: List[Dict[str, object]] = [
    cat_config("cat_cua"),
    cat_config("cat_estado"),
    cat_config("cat_marca"),
    cat_config("cat_sucursal"),
    cat_config("cat_tipo_vehiculo"),
    cat_config("cat_tipo_vigencia"),
    cat_config("cat_uen"),
    cat_config("cat_uso"),
    cat_config("cat_conducto_cobro"),
    cat_config("cat_municipio"),
    {
        "target_table": "tbl_poliza",
        "raw_path": f"{RAW_BASE}/tbl_poliza_raw/",
        "target_path": f"{STANDARD_BASE}/tbl_poliza/",
        "format": "parquet",
        "transform": transform_tbl_poliza,
        "partition_keys": [],
    },
    {
        "target_table": "tbl_certificado",
        "raw_path": f"{RAW_BASE}/tbl_certificado/",
        "target_path": f"{STANDARD_BASE}/tbl_certificado/",
        "format": "parquet",
        "transform": transform_tbl_certificado,
        "partition_keys": [],
    },
    {
        "target_table": "tbl_certificado_ramo",
        "raw_path": f"{RAW_BASE}/tbl_certificado_ramo_raw/",
        "target_path": f"{STANDARD_BASE}/tbl_certificado_ramo/",
        "format": "parquet",
        "transform": transform_tbl_certificado_ramo,
        "partition_keys": ["year"],
    },
    {
        "target_table": "tbl_detalle_vehiculo",
        "raw_path": f"{RAW_BASE}/tbl_detalle_vehiculo_raw/",
        "target_path": f"{STANDARD_BASE}/tbl_detalle_vehiculo/",
        "format": "parquet",
        "transform": transform_tbl_detalle_vehiculo,
        "partition_keys": [],
    },
    {
        "target_table": "tbl_movimiento_endoso",
        "raw_path": f"{RAW_BASE}/tbl_movimiento_endoso_raw/",
        "target_path": f"{STANDARD_BASE}/tbl_movimiento_endoso/",
        "format": "parquet",
        "transform": transform_tbl_movimiento_endoso,
        "partition_keys": [],
    },
    {
        "target_table": "tbl_cobertura",
        "raw_path": f"{RAW_BASE}/tbl_cobertura_raw/",
        "target_path": f"{STANDARD_BASE}/tbl_cobertura/",
        "format": "parquet",
        "transform": transform_tbl_cobertura,
        "partition_keys": [],
    },
    {
        "target_table": "tbl_descuento_poliza_cert",
        "raw_path": f"{RAW_BASE}/tbl_descuento_poliza_cert_raw/",
        "target_path": f"{STANDARD_BASE}/tbl_descuento_poliza_cert/",
        "format": "parquet",
        "transform": transform_tbl_descuento_poliza_cert,
        "partition_keys": [],
    },
    {
        "target_table": "hc_detalle_siniestralidad",
        "raw_path": f"{RAW_BASE}/hc_detalle_siniestralidad/",
        "target_path": f"{STANDARD_BASE}/hc_detalle_siniestralidad/",
        "format": "parquet",
        "transform": transform_hc_detalle_siniestralidad,
        "partition_keys": [],
    },
]


DERIVED_CONFIGS: List[Dict[str, object]] = [
    {
        "target_table": "tbl_renovaciones_certificado",
        "target_path": f"{STANDARD_BASE}/tbl_renovaciones_certificado/",
        "format": "parquet",
        "builder": build_tbl_renovaciones_certificado,
        "partition_keys": [],
    },
    {
        "target_table": "tbl_renovaciones_cobertura",
        "target_path": f"{STANDARD_BASE}/tbl_renovaciones_cobertura/",
        "format": "parquet",
        "builder": build_tbl_renovaciones_cobertura,
        "partition_keys": [],
    },
]


def selected(table_name: str) -> bool:
    requested = (args["TABLES"] or "all").strip().lower()
    if requested == "all":
        return True
    wanted = {item.strip().lower() for item in requested.split(",") if item.strip()}
    return table_name.lower() in wanted


def process_base_table(config: Dict[str, object]) -> None:
    target_table = config["target_table"]
    if not selected(target_table):
        return
    print(f"\n=== Procesando {target_table} ===")
    if str(args["PROCESS_DATA"]).lower() == "true":
        df = read_source(config)
        transform: Transform = config["transform"]
        result = transform(df)
        write_catalog_table(result, config)
    maybe_update_catalog_location(config)


def process_derived_table(config: Dict[str, object]) -> None:
    target_table = config["target_table"]
    if not selected(target_table):
        return
    print(f"\n=== Procesando derivada {target_table} ===")
    if str(args["PROCESS_DATA"]).lower() == "true":
        result = config["builder"]()
        write_catalog_table(result, config)
    maybe_update_catalog_location(config)


for table_config in TABLE_CONFIGS:
    process_base_table(table_config)

if str(args["INCLUDE_DERIVED"]).lower() == "true":
    for derived_config in DERIVED_CONFIGS:
        process_derived_table(derived_config)

job.commit()
