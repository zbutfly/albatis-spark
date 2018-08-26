package net.butfly.albatis.spark.output;

import static net.butfly.albatis.spark.impl.Schemas.rmap2row;
import static net.butfly.albatis.spark.impl.SchemaExtraField.purge;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.hzcominfo.albatis.nosql.Connection;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.SparkOutput;
import net.butfly.albatis.spark.impl.Schemas;
import net.butfly.albatis.spark.util.DSdream;

class WriteHandlerFrame extends WriteHandlerBase<WriteHandlerFrame> {
	protected WriteHandlerFrame(TableDesc table, Dataset<Rmap> ds) {
		super(rmap2row(table, ds));
	}

	protected WriteHandlerFrame(Dataset<Row> ds) {
		super(ds);
	}

	@Override
	public void save(String format, Map<String, String> options) {
		options.putIfAbsent("checkpointLocation", checkpoint());
		purge(ds).write().format(format).options(options).save();
	}

	@Override
	public void save(String table, Output<Rmap> output) {
		if (output instanceof SparkOutput) output.enqueue(DSdream.of(table, purge(ds)));
		else {// should not be touch
			purge(ds).foreachPartition(it -> {
				try (Connection cc = output.connect();) {
					// TODO: split
					output.enqueue(Sdream.of(it).map(Schemas::row2rmap));
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			});
		}
	}
}
