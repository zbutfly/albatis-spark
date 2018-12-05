package net.butfly.albatis.spark;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import scala.Tuple2;

public abstract class SparkMapInput extends SparkInput<Rmap> {
	private static final long serialVersionUID = -2144747945365613002L;

	protected SparkMapInput(SparkSession spark, URISpec targetUri, TableDesc... table) {
		super(spark, targetUri, table);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected abstract List<Tuple2<String, Dataset<Rmap>>> load();

	@Override
	protected final DatasetMode mode() {
		return DatasetMode.RMAP;
	}
}
