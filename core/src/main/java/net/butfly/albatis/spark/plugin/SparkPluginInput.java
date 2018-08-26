package net.butfly.albatis.spark.plugin;

import static net.butfly.albatis.spark.impl.Sparks.alias;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.max;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.spark.SparkInput;
import scala.Tuple2;

public class SparkPluginInput extends SparkInput<Row> {
	private static final long serialVersionUID = -3514105763334222049L;
	protected final SparkInput<Row> input;
	protected final PluginConfig pc;
	protected final static String PLUGIN_KEY = PluginElement.zjhm.name();
	protected final static String COUNT = PluginElement.count.name();
	protected final static String MAX_SCORE = PluginElement.max_score.name();

	public SparkPluginInput(SparkInput<Row> input, PluginConfig pc) {
		super(input.spark, input.targetUri);
		this.input = input;
		this.pc = pc;
	}

	@Override
	public void open() {
		input.open();
		super.open();
	}

	@SuppressWarnings("unchecked")
	@Override
	protected List<Tuple2<String, Dataset<Row>>> load() {
		String maxScore = pc.getMaxScore();
		return Colls.flat(Colls.list(pc.getKeys(), (Function<String, List<Tuple2<String, Dataset<Row>>>>) k -> Colls.list(input.rows(),
				t -> new Tuple2<>(t._1, t._2.groupBy(col(k).as(PLUGIN_KEY)).agg(count("*").as(COUNT), max(maxScore).as(MAX_SCORE))))));
	}

	@Override
	public void close() {
		super.close();
		input.close();
	}
}
