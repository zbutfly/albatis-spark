package net.butfly.albatis.spark.output;

import java.util.Map;

import org.apache.spark.sql.Dataset;

import com.hzcominfo.albatis.nosql.Connection;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;

class WriteHandlerFrame extends WriteHandlerBase<WriteHandlerFrame> {
	protected WriteHandlerFrame(Dataset<Rmap> ds) {
		super(ds);
	}

	@Override
	public void save(String format, Map<String, String> options) {
		options.putIfAbsent("checkpointLocation", checkpoint());
		ds.write().format(format).options(options).save();
	}

	@Override
	public void save(Output<Rmap> output) {
		// try (Connection cc = output.connect();) {
		// output.enqueue(DSdream.of(ds));
		// } catch (Exception e) {
		// throw new RuntimeException(e);
		// }
		ds.foreachPartition(it -> {
			try (Connection cc = output.connect();) {
				output.enqueue(Sdream.of(it));
			}
		});
	}
}
