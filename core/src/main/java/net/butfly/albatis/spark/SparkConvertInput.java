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
//	private SparkSession sparksc = null;

    public SparkConvertInput(SparkInput<Rmap> input, SparkSession sparksc, List<Map<String, Object>> allDocs, String tableName, URISpec uri, Map<String, String> fieldSet) throws IOException {
        super(null,null,Maps.of("sparksc",sparksc,"allDocs",allDocs,"tableName",tableName,"fieldSet",fieldSet));
//        this.sparksc = sparksc;
//		super(input.spark,input.targetUri,Maps.of("allDocs",allDocs,"tableName",tableName,"fieldSet",fieldSet));
    }

	@Override
	protected List<Tuple2<String, Dataset<Row>>> load(Object context) throws IOException {
		Map<String, Object> config = (Map<String, Object>) context;
		Encoder<Rmap> rowEncoder = Encoders.kryo(Rmap.class);
//      should convert map to row, map to rmap ,thne make it row;
        List<Map<String, Object>> esResult = (List<Map<String, Object>>) config.get("allDocs");
        if (esResult.size() < 1)
            throw new IllegalArgumentException("esResult is empty");
        List<Rmap> rmapList = esResult.parallelStream().map(x -> new Rmap().map(x)).collect(Collectors.toList());
        URISpec hotel1URI = new URISpec("es:rest://hzcominfo@172.30.10.31:39200/hotel_info_2");
		DBDesc dbDesc = DBDesc.of("hotel_info_2",hotel1URI.getPath()); //TODO this.targetUri.getFile()
		TableDesc tableDesc = dbDesc.table(String.valueOf(config.get("fieldSet")));
		List<String> fieldList = new ArrayList<String>( esResult.get(0).keySet()); //TODO allDocs is not empty
		for (int i =0;i<fieldList.size();i++) {
//            ValType type = ValType.of("string");
			FieldDesc fieldDesc = new FieldDesc(tableDesc, fieldList.get(i), null); //
			tableDesc.field(fieldDesc);
		}
		long createDSStart = System.currentTimeMillis();
        SparkSession sparkss = (SparkSession) config.get("sparksc");
		Dataset<Rmap> dataset = sparkss.createDataset(rmapList, rowEncoder);//TODO  first get spark
		Dataset<Row> ds = rmap2row(tableDesc,dataset); //TODO tableDesc should has struct slowly
		Dataset<Row> aliasDS = getAliasDS(ds, (Map<String, String>) config.get("fieldSet"));
		logger().info("<convertDs>"+aliasDS.schema().treeString());
//		logger().info("convert list to ds use:"+(System.currentTimeMillis() - createDSStart)+"ms");

		long countStart = System.currentTimeMillis();
		long count = aliasDS.cache().count();
		logger().info("count use:"+(System.currentTimeMillis()-countStart)+"ms"+"\t"+"convertDS count:"+count);
        return Colls.list(new Tuple2<>(String.valueOf(config.get("table")), aliasDS));
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
