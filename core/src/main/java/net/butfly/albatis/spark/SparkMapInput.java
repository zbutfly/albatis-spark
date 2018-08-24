package net.butfly.albatis.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;

public abstract class SparkMapInput extends SparkInput<Rmap> {
	private static final long serialVersionUID = -2144747945365613002L;

	protected SparkMapInput(SparkSession spark, URISpec targetUri, TableDesc... table) {
		super(spark, targetUri, table);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected abstract Dataset<Rmap> load();

	@Override
	protected final DatasetMode mode() {
		return DatasetMode.RMAP;
	}

}
