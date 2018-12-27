package net.butfly.albatis.spark.output;

import static net.butfly.albatis.spark.impl.Schemas.rmap2row;
import static net.butfly.albatis.spark.impl.SchemaExtraField.purge;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import net.butfly.albatis.Connection;

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
//		传入table名,Output对象.如果output对象是SparkOutput的实例,就直接把DSdream对象放入output对象里,否则就要把dataset用purge处理遍历后
		if (output instanceof SparkOutput) output.enqueue(DSdream.of(table, purge(ds)));
		else {// should not be touch
			purge(ds).foreachPartition(it -> {
				try (Connection cc = output.connect()) {
					// TODO: split
					output.enqueue(Sdream.of(it).map(Schemas::row2rmap));
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			});
		}
	}

//	public void save(String table,Output<Rmap> output){
//		if (output instanceof SparkOutput) output.enqueue(DSdream.of(table,purge(ds)));
//		else {
//			purge(ds).foreachPartition(it -> {
//				try(Connection cc = output.connect()){
//					output.enqueue(Sdream.of(it).map(Schemas::row2rmap));
//				}catch (Exception e){
//					throw new RuntimeException(e);
//				}
//			});
//		}
//	}


}
