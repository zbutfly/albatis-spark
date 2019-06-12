package net.butfly.albatis.spark;

import java.io.IOException;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.TableDesc;
import scala.Tuple2;

public abstract class SparkRowInput extends SparkInput<Row> {
	private static final long serialVersionUID = -2144747945365613002L;

	protected SparkRowInput(SparkSession spark, URISpec targetUri, Object context, TableDesc... table) throws IOException {
		super(spark, targetUri, context, table);
	}

	protected List<Tuple2<String, Dataset<Row>>> load() throws IOException {
		return load(Maps.of());
	}

	@SuppressWarnings("unchecked")
	@Override
	protected <T> List<Tuple2<String, Dataset<T>>> load(Object context) throws IOException {
		return Colls.list(load(), t -> new Tuple2<>(t._1, (Dataset<T>) t._2));
	}

	@Override
	protected final DatasetMode mode() {
		return DatasetMode.ROW;
	}
}