package net.butfly.albatis.spark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.Rmap;
import scala.Tuple2;
import scala.collection.JavaConverters;

public final class SparkJoinInput extends SparkRowInput {
	private static final long serialVersionUID = -4870210186801499L;
	public final String joinCol; // col to upper joining, not the finished join
	private final SparkInput<Rmap> left;
	private final SparkInput<Rmap> right;

	public SparkJoinInput(SparkInput<Rmap> left, String lcol, SparkInput<Rmap> right, String rcol, SparkJoinType type, String as) throws IOException {
		super(left.spark, left.targetUri, Maps.of("left", left, "right", right, "lcol", lcol, "rcol", rcol, "type", type,"targetTable",as));
		this.left = left;
		this.joinCol = lcol;
		this.right = right;
	}

	@Override
	public void open() {
		right.open();
		left.open();
		super.open();
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<Tuple2<String, Dataset<Row>>> load(Object context) {
		Map<String, Object> ctx = (Map<String, Object>) context;
		List<Tuple2<String, Dataset<Row>>> rows;
		rows = ((SparkInput<Rmap>) ctx.get("left")).rows;
		if (rows.size() > 1) //
			logger().warn("Input with multiple datasets not support, only first joined and other is ignored.\n\t" + rows);
		Dataset<Row> left = rows.get(0)._2;
		rows = ((SparkInput<Rmap>) ctx.get("right")).rows;
		if (rows.size() > 1) //
			logger().warn("Input with multiple datasets not support, only first joined and other is ignored.\n\t" + rows);
		Dataset<Row> right = rows.get(0)._2;
		SparkJoinType t = (SparkJoinType) ctx.get("type");
		long joinStart = System.currentTimeMillis();
		Dataset<Row> joinDS = t.join(left, (String) ctx.get("lcol"), right, (String) ctx.get("rcol"));  //TODO will general new stage,give new partition
		logger().info("joinDS use:\t"+(System.currentTimeMillis()-joinStart) + "ms");

		logger().info("Join [" + t + "]: \n\t<left:>" + left.schema().treeString() + //
				"\t<right:>" + right.schema().treeString() + //
				"\t<result:>" + joinDS.schema().treeString());
		logger().trace(() -> "Loaded from elasticsearch, schema: " + joinDS.schema().treeString());

		long joinafterStart = System.currentTimeMillis();
//		joinDS.cache();
//		joinDS.count();
//		logger().info("join after count use:"+(System.currentTimeMillis()-joinafterStart)/1000.0+"s");
		logger().debug("Dataset loaded.");
		return Colls.list(new Tuple2<>((String) ctx.get("targetTable"), joinDS));
	}

	@Deprecated
	protected final Dataset<Row> filterCols(Dataset<Row> ds, String leftTable, Dataset<Row> left, String rightTable, Dataset<Row> right) {
		List<Column> allRows = new ArrayList<>();
		for (String col : left.columns()) {
			Column c = left.col(col);
			if (null != leftTable) c = c.as(addPrefix(leftTable, col));// right is real table
			allRows.add(c);
		}
		for (String col : right.columns()) {
			Column c = right.col(col);
			if (null != rightTable) c = c.as(addPrefix(rightTable, col));// right is real table
			allRows.add(c);
		}
		if (!allRows.isEmpty()) ds = ds.select(JavaConverters.asScalaIteratorConverter(allRows.iterator()).asScala().toSeq());
		return ds;
	}

	@Deprecated
	private static String addPrefix(String tableName, String oldColumn) {
		String resultName = tableName + "_" + oldColumn;
		return resultName;
	}

	@Override
	public void close() {
		super.close();
		left.close();
		right.close();
	}
}
