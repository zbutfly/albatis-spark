package net.butfly.albatis.spark.plugin;

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.spark.SparkInput;
import scala.Tuple2;

public class SparkCollisionPluginInput extends SparkPluginInput {
	private static final long serialVersionUID = -8270271411455957886L;
	private final Map<SparkInput<Row>, String> cInputs;

	public SparkCollisionPluginInput(SparkInput<Row> input, PluginConfig pc) {
		super(input, pc);
		Map<SparkInput<Row>, String> cInputs = pc.getCollisionInputs();
		if (cInputs == null || cInputs.isEmpty()) throw new RuntimeException("Not conforming to the conditions of collision plugin");
		this.cInputs = cInputs;
	}

	@Override
	public void open() {
		for (SparkInput<Row> in : cInputs.keySet())
			in.open();
		super.open();
	}

	@Override
	protected List<Tuple2<String, Dataset<Row>>> load() {
		List<List<List<Tuple2<String, Dataset<Row>>>>> list = Colls.list(super.load(), //
				t -> Colls.list(cInputs.entrySet(), e -> Colls.list(e.getKey().rows(), //
						t1 -> new Tuple2<>(t._1 + "*" + t1._1, //
								t._2.join(t1._2, t._2.col(PLUGIN_KEY).equalTo(t1._2.col(e.getValue())), "inner")))));
		return Colls.flat(Colls.flat(list));
	}

	@Override
	public void close() {
		super.close();
		for (SparkInput<Row> in : cInputs.keySet())
			in.close();
	}
}
