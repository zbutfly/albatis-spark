package net.butfly.albatis.spark.input;

import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.SparkInput;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.util.List;
import java.util.Set;

public class SparkInnerJoinInput extends SparkJoinInput {
	private static final long serialVersionUID = 377289278732441635L;

	public final SparkInput<Rmap> input;
	public final String col;
	public final SparkInput<Rmap> joined;
	public final String joinedCol;

	public SparkInnerJoinInput(SparkInput<Rmap> input, String col, SparkInput<Rmap> joined, String joinedCol, String as, Set<String> leftSet, Set<String> rightSet, String taskId) {
		super(input, col, joined, joinedCol, "inner", as,leftSet,rightSet,taskId);
		this.input = input;
		this.col = col;
		this.joined = joined;
		this.joinedCol = joinedCol;
	}

	@Override
	public List<Tuple2<String, Dataset<Row>>> load(Object context) {
		return super.load(context);
	}
}
