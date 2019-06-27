package net.butfly.albatis.mongodb;

import static net.butfly.albatis.spark.impl.SchemaExtraField.FIELD_KEY_VALUE;
import static net.butfly.albatis.spark.impl.SchemaExtraField.FIELD_TABLE_NAME;
import static net.butfly.albatis.spark.impl.Sparks.logger;
import static net.butfly.albatis.spark.impl.Sparks.split;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Strings;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import static org.apache.spark.sql.functions.lit;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.Systems;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.Albatis;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.spark.SparkRowInput;
import net.butfly.albatis.spark.impl.SparkConf;
import net.butfly.albatis.spark.impl.SparkIO.Schema;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

@Schema("mongodb")
@SparkConf(key = "spark.mongodb.input.uri", value = "mongodb://127.0.0.1/FxxkMongoSpark.FakeCollection")
public class SparkMongoInput extends SparkRowInput implements SparkMongo {
	private static final long serialVersionUID = 2110132305482403155L;

	public SparkMongoInput(SparkSession spark, URISpec targetUri, TableDesc... table) throws IOException {
		super(spark, targetUri, null, table);
	}

	@Override
	public Map<String, String> options() {
		Map<String, String> opts = mongoOpts(targetUri);
		opts.put("partitioner", "MongoSamplePartitioner");
		return opts;
	}

	@Override
	protected List<Tuple2<String, Dataset<Row>>> load() {
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		List<List<Tuple2<String, Dataset<Row>>>> resultList = Colls.list(schemaAll().values(), item -> {
			Map<String, String> opts = options();
			opts.put("collection", item.name);
			ReadConfig rc = ReadConfig.create(opts);
			FieldDesc[] fields = item.fields();
			List<String> fieldList = new ArrayList<>();
			for (FieldDesc desc : fields) {
				String name = desc.attr("asfrom");
				fieldList.add(name);
			}
			long start = System.currentTimeMillis();
			JavaMongoRDD<Document> rdd = MongoSpark.load(jsc, rc);
			if (Systems.isDebug()) {
				int limit = Integer.parseInt(Configs.gets(Albatis.PROP_DEBUG_INPUT_LIMIT, "-1")) / rdd.getNumPartitions() + 1;
				if (limit > 0) rdd = rdd.withPipeline(Colls.list(Document.parse("{ $limit: " + limit + " }")));
			}
			Dataset<Row> ds = rdd.toDF().cache();
			logger().info("mongo cache use:"+ (System.currentTimeMillis()-start)/1000.0 + "s");
			String table_queryparam = (String) item.attr("TABLE_QUERYPARAM");
			Dataset<Row> resultDS =null;
			if (Strings.isNullOrEmpty(table_queryparam)){
				resultDS = ds;
			}else{
				resultDS = ds.where(table_queryparam);
			}
			resultDS = resultDS.withColumn(FIELD_TABLE_NAME, lit(item.name)).withColumn(FIELD_KEY_VALUE, ds.col("_id.oid"));
			String[] columns = resultDS.columns();  //if ds has _id column ,drop it; it lead to codec problem
			for (String column : columns) {
				if (column.equals("_id")){
					resultDS = resultDS.drop(column);
				}
			}
//			resultDS = resultDS.withColumn(FIELD_TABLE_NAME, lit(item.name)).withColumn(FIELD_KEY_VALUE, ds.col("_id.oid")).withColumn("_id", ds.col("_id.oid"));
			long count = resultDS.persist(StorageLevel.MEMORY_AND_DISK()).count();
			logger().info("MongoSpark load use:\t"+ (System.currentTimeMillis()-start)/1000.0 + "s"+"\n\tcount:\t"+count);
			return Colls.list(split(resultDS, false), ds1 -> new Tuple2<>(item.name, ds1));
		});
		return flat(resultList);
	}

	public Seq<Column> convertListToSeq(List<Column> inputList) {
		return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
	}

	static <E> List<E> flat(Iterable<List<E>> l) {
		List<E> ll = Colls.list();
		l.forEach(l0 -> ll.addAll(l0));
		return ll;
	}
}
