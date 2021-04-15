package mx.com.gnp.datalake.gcp.autos.emision.noeconomico;


import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

public class AutDatosAsegurado implements SparkSessionWrapper {
    public static Logger log = LogManager.getLogger(AutDatosAsegurado.class);

    public static final String CARGA_DELTA = "delta";

    public static final String CARGA_COMPLETA = "completa";

    public static void main(String[] args) {

        log.info("########################## Artefacto AUT_DATOS_ASEGURADO ##########################");
        String rutaCrudos = "gs://gnp-dlk-pro-crudos/info";
        String rutaModelado = "gs://gnp-dlk-pro-modelado/autos/emision/no_economico";
        String rutaResultado = rutaModelado + "/AUT_DATOS_ASEGURADO";
        Dataset<Row> autElementoObjetoTP = null, kpem08c = null, autDatosContacto = null;
        autElementoObjetoTP = spark.read().format("avro").load(rutaModelado + "/AUT_ELEMENTO_OBJETO_TP/data/*.avro");
        kpem08c = spark.read().format("avro").load(rutaCrudos + "/KPEM08C/data/*.avro");
        autDatosContacto = spark.read().format("avro").load(rutaModelado + "/AUT_DATOS_CONTACTO/data/*.avro");
        log.info("######## 2 CREAR TABLAS EN MEMORIA, DATASETS-SUBQUERYs, UNION TABLAS ########");
        autElementoObjetoTP.createOrReplaceTempView("TABLA_AUT_ELEMENTOOBJETO_TP");
        kpem08c.createOrReplaceTempView("TABLA_KPEM08C");
        autDatosContacto.createOrReplaceTempView("TABLA_AUT_DATOSCONTACTO");
        Dataset<Row> pasounico = spark.sql("SELECT\n" +
                "  DISTINCT NUM_POLIZA,\n" +
                "  NUM_VERSION,\n" +
                "  A.CVE_FILIACION AS CVE_ASEGURADO,\n" +
                "  CAST(B.CDRFC AS CHAR(20)) AS RFC,\n" +
                "  B.TCPEFIJU AS TIPO_PARTICIPANTE,\n" +
                "case\n" +
                "    when trim(B.TCPEFIJU) = 'F' then 'Fisica'\n" +
                "    else 'Moral'\n" +
                "  end as desc_tipo_persona,\n" +
                "  TRIM(regexp_replace(REPLACE(B.DNNOMRZM,CHAR(9), ' '),'#','Ñ')) AS NOMBRE,\n" +
                "  TRIM(regexp_replace(REPLACE(B.DNAP1RZM,CHAR(9), ' '),'#','Ñ')) AS PRIMER_APELLIDO,\n" +
                "  TRIM(regexp_replace(REPLACE(B.DNAP2RZM,CHAR(9), ' '),'#','Ñ')) AS SEGUNDO_APELLIDO,\n" +
                "CASE\n" +
                "    WHEN upper(B.TCPEFIJU) = 'F' THEN  trim(concat(\n" +
                "      case when DNNOMRZM is null then '' else TRIM(regexp_replace(REPLACE(DNNOMRZM,CHAR(9), ' '),'#','Ñ')) end,\n" +
                "      ' ',\n" +
                "      case when DNAP1RZM is null then '' else TRIM(regexp_replace(REPLACE(DNAP1RZM,CHAR(9), ' '),'#','Ñ')) end,\n" +
                "      ' ',\n" +
                "      case when DNAP2RZM is null then '' else TRIM(regexp_replace(REPLACE(DNAP2RZM,CHAR(9), ' '),'#','Ñ')) end\n" +
                "    ))\n" +
                "    ELSE trim(concat(\n" +
                "      case when DNAP1RZM is null then '' else TRIM(regexp_replace(REPLACE(DNAP1RZM,CHAR(9), ' '),'#','Ñ')) end,\n" +
                "      ' ',\n" +
                "      case when DNAP2RZM is null then '' else TRIM(regexp_replace(REPLACE(DNAP2RZM,CHAR(9), ' '),'#','Ñ')) end,\n" +
                "      ' ',\n" +
                "      case when DNNOMRZM is null then '' else TRIM(regexp_replace(REPLACE(DNNOMRZM,CHAR(9), ' '),'#','Ñ')) end\n" +
                "    )) \n" +
                "  END AS NOMBRE_COMPLETO,\n" +
                "  CAST('ASEGURADO' AS CHAR(9)) AS IND_ROL,\n" +
                "  c.calle,\n" +
                "  c.numero,\n" +
                "  c.ref_adicional_domicilio,\n" +
                "  c.cvecolonia,\n" +
                "  c.colonia,\n" +
                "  c.delegacion_o_municipio,\n" +
                "  c.entidad_federativa,\n" +
                "  c.codigo_postal,\n" +
                "  c.codigo_pais,\n" +
                "  c.correo_electronico,\n" +
                "  c.tipo_comunicacion_1,\n" +
                "  c.numero_prefijo_pais_1,\n" +
                "  c.numero_prefijo_celular_1,\n" +
                "  c.numero_prefijo_ciudad_1,\n" +
                "  c.numero_telefono_1,\n" +
                "  c.numero_extension_telefono_1,\n" +
                "  c.tipo_comunicacion_2,\n" +
                "  c.numero_prefijo_pais_2,\n" +
                "  c.numero_prefijo_celular_2,\n" +
                "  c.numero_prefijo_ciudad_2,\n" +
                "  c.numero_telefono_2,\n" +
                "  c.numero_extension_telefono_2,\n" +
                "  B.TSCDC\n" +
                "FROM\n" +
                "  TABLA_AUT_ELEMENTOOBJETO_TP A\n" +
                "  left JOIN TABLA_KPEM08C B ON A.CVE_FILIACION = B.CDFILIAC\n" +
                "  left join TABLA_AUT_DATOSCONTACTO c on a.CVE_FILIACION = c.cve_filiacion\n" +
                "where\n" +
                "  A.CVE_FILIACION is not null\n" +
                "  and trim(A.CVE_FILIACION) <> ''");
        pasounico.dropDuplicates("num_poliza", "num_version")
                .write().mode(SaveMode.Overwrite).format("avro")
                .save(rutaResultado + "/data");
        log.info("########################## TERMINA Artefacto AUT_DATOS_ASEGURADO ##########################");

    }
}
