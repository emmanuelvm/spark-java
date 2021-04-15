package mx.com.gnp.datalake.gcp.autos.emision.noeconomico.transformations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

public class Transformations {

    public long myCounter(Dataset<Row> df){
        return df.count();
    }
    
    public String[] getCleanSchema(Dataset<Row> df){
    	String [] schema = null;
    	StructType st = null;
    	if(df != null){
    		st = df.schema();
    		schema = new String[st.fieldNames().length -2];
    		for(int i=0; i<st.fieldNames().length;i++){
    			if(!st.fieldNames()[i].equals("TSCDC") && !st.fieldNames()[i].equals("TSULTMOD"))
    				schema[i]=st.fieldNames()[i];
    		}
    	}
    	return schema;
    }
}