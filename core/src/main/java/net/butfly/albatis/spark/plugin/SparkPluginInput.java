package net.butfly.albatis.spark.plugin;

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
		super(input.spark, input.targetUri, null);
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
	protected List<Tuple2<String, Dataset<Row>>> load(Object context) {
//		拿到maxscore,压平PluginConfig, 传入table1对象,返回一个list
		String maxScore = pc.getMaxScore();
		return Colls.flat(Colls.list(pc.getKeys(), (Function<String, List<Tuple2<String, Dataset<Row>>>>) table1 -> Colls.list(input.rows(),
//			    传入一个table2对象,返回一个tuple, 第二个元素是按照他t1分组,再聚合,求count,求max
				table2 -> new Tuple2<>(table2._1, table2._2.groupBy(col(table1).as(PLUGIN_KEY)).agg(count("*").as(COUNT), max(maxScore).as(MAX_SCORE))))));
	}

	@Override
	public void close() {
		super.close();
		input.close();
	}
}
