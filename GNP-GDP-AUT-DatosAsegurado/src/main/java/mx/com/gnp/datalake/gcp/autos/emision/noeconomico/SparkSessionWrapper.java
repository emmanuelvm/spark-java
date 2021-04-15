package mx.com.gnp.datalake.gcp.autos.emision.noeconomico;

import org.apache.spark.sql.SparkSession;

public interface SparkSessionWrapper {

    SparkSession spark = SparkSession
            .builder()
            .appName("AUT_DATOS_ASEGURADO")
            .config("spark.sql.avro.compression.codec", "snappy")
		    .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
		    .config("spark.shuffle.blockTransferService", "nio")
            .getOrCreate();
}