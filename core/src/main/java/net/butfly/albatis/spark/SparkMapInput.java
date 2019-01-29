package net.butfly.albatis.spark;

import java.io.IOException;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import scala.Tuple2;

public abstract class SparkMapInput extends SparkInput<Rmap> {
	private static final long serialVersionUID = -2144747945365613002L;

	protected SparkMapInput(SparkSession spark, URISpec targetUri, TableDesc... table) throws IOException {
		super(spark, targetUri, null, table);
	}

	protected abstract List<Tuple2<String, Dataset<Rmap>>> load();

	@SuppressWarnings("unchecked")
	@Override
	protected final <T> List<Tuple2<String, Dataset<T>>> load(Object context) {
		return Colls.list(load(), t -> new Tuple2<>(t._1, (Dataset<T>) t._2));
	}

	@Override
	protected final DatasetMode mode() {
		return DatasetMode.RMAP;
	}

}
