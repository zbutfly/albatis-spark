package net.butfly.albatis.spark;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.ddl.TableDesc;
import scala.Tuple2;

public abstract class SparkRowInput extends SparkInput<Row> {
	private static final long serialVersionUID = -2144747945365613002L;

	protected SparkRowInput(SparkSession spark, URISpec targetUri, TableDesc... table) {
		super(spark, targetUri, table);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected abstract List<Tuple2<String, Dataset<Row>>> load();

	@Override
	protected final DatasetMode mode() {
		return DatasetMode.ROW;
	}
}