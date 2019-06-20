package net.butfly.albatis.spark;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.DBDesc;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.SparkInput;
import net.butfly.albatis.spark.SparkJoinType;
import net.butfly.albatis.spark.SparkRowInput;
import org.apache.spark.sql.*;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static net.butfly.albatis.spark.impl.Schemas.rmap2row;

public final class SparkConvertInput extends SparkRowInput {
	private static final long serialVersionUID = -4870210186801496L;

    public SparkConvertInput(SparkInput<Rmap> input,  List<Map<String, Object>> allDocs, String tableName, URISpec uri, Map<String, String> fieldSet) throws IOException {
        super(null,null,Maps.of("allDocs",allDocs,"tableName",tableName,"fieldSet",fieldSet));
//		super(input.spark,input.targetUri,Maps.of("allDocs",allDocs,"tableName",tableName,"fieldSet",fieldSet));
    }

	@Override
	protected List<Tuple2<String, Dataset<Row>>> load(Object context) throws IOException { //TODO context should contains
		Map<String, Object> config = (Map<String, Object>) context;
		Encoder<Rmap> rowEncoder = Encoders.kryo(Rmap.class);
//      should convert map to row, map to rmap ,thne make it row;
        List<Map<String, Object>> esResult = (List<Map<String, Object>>) config.get("allDocs");
        if (esResult.size() < 1)
            throw new IllegalArgumentException("esResult is empty");
        List<Rmap> rmapList = esResult.parallelStream().map(x -> new Rmap().map(x)).collect(Collectors.toList()); //TODO get from Obj
//        context
        URISpec hotel1URI = new URISpec("es:rest://hzcominfo@172.30.10.31:39200/hotel_info_2");
		DBDesc dbDesc = DBDesc.of("hotel_info_2",hotel1URI.getPath()); //TODO this.targetUri.getFile()
		TableDesc tableDesc = dbDesc.table(String.valueOf(config.get("fieldSet")));
		List<String> fieldList = new ArrayList<String>( esResult.get(0).keySet()); //TODO allDocs is not empty
		for (int i =0;i<fieldList.size();i++) { //TODO cut field
//            ValType type = ValType.of("string");
			FieldDesc fieldDesc = new FieldDesc(tableDesc, fieldList.get(i), null);
			tableDesc.field(fieldDesc);
		}
		long createDSStart = System.currentTimeMillis();
		Dataset<Rmap> dataset = spark.createDataset(rmapList, rowEncoder);//TODO  first get spark
		logger().info("create ds use:"+(System.currentTimeMillis() - createDSStart)+"ms");
		Dataset<Row> ds = rmap2row(tableDesc,dataset); //TODO tableDesc should has struct
		Dataset<Row> aliasDS = getAliasDS(ds, (Map<String, String>) config.get("fieldSet"));
        String table = String.valueOf(config.get("table"));
//        List<Tuple2<String, Dataset<T>>> list = Colls.list(new Tuple2<>(table,aliasDS));
        return Colls.list(new Tuple2<>(table, aliasDS));
	}

	public Dataset<Row> getAliasDS(Dataset<Row> ds, Map<String, String> fieldMap) {
		List<Column> allRows = new ArrayList<>();
		for (String col : ds.columns()) {
			String alias = fieldMap.get(col);
			if (null != alias){
				Column c = ds.col(col);
				c = c.as(alias);
				allRows.add(c);
			}
		}
		if (!allRows.isEmpty()) ds = ds.select(JavaConverters.asScalaIteratorConverter(allRows.iterator()).asScala().toSeq());
		return ds;
	}

}
