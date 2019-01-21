package net.butfly.albatis.spark.input;

import java.util.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.SparkInput;
import net.butfly.albatis.spark.SparkRowInput;
import scala.Tuple2;
import scala.collection.JavaConverters;

public class SparkJoinInput extends SparkRowInput {
	private static final long serialVersionUID = -4870210186801499L;
	public final SparkInput<Rmap> input;
	public final String col;
	public final SparkInput<Rmap> joined;
	public final String joinedCol;
	public final String joinType;

	public SparkJoinInput(SparkInput<Rmap> input, String col, SparkInput<Rmap> joined, String joinedCol, String joinType, String asTable, Set<String> leftSet, Set<String> rightSet) {
		super(input.spark, input.targetUri, Maps.of("leftInput", input, "rightInput", joined, "leftCol", col, "rightCol", joinedCol, "type", joinType, "as", asTable,"leftSet",leftSet,"rightSet",rightSet));
		this.input = input;
		this.col = col;
		this.joined = joined;
		this.joinedCol = joinedCol;
		this.joinType = joinType;
	}

	@Override
	public void open() {
		joined.open();
		input.open();
		super.open();
	}

	public scala.collection.Seq<String> convertListToSeq(List<String> inputList) {
		return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<Tuple2<String, Dataset<Row>>> load(Object context) {
		Map<String, Object> ctx = (Map<String, Object>) context;
		// 拿到fieldSet 拿到leftInput的全字段; 对ctx.get(i)做drop
		SparkInput<Rmap> leftInput = (SparkInput<Rmap>) ctx.get("leftInput");
		String leftTableName = leftInput.rows().get(0)._1;
		Set<String> leftSet = (Set<String>) ctx.get("leftSet");
		String leftjoinCol = (String) ctx.get("leftCol");
		// 拿到input的表名 tableName应该是table()

//		String tableName = leftInput.targetUri.getFile();
		Dataset<Row> purgedLeftDS = getPurgedDS(leftInput, leftSet, leftjoinCol);
//		要拿到表名,ds,传入
//		Dataset<Row> renameDS = renameDS(purgedLeftDS, leftTableName);

		List<Tuple2<String, Dataset<Row>>> leftRows = Colls.list(new Tuple2<>(leftTableName, purgedLeftDS));
//		purgedLeftDS.show(1);

        SparkInput<Rmap> rightInput = (SparkInput<Rmap>) ctx.get("rightInput");
		String rightTableName = rightInput.rows().get(0)._1;
		Set<String> rightSet = (Set<String>) ctx.get("rightSet");
		String rightCol = (String) ctx.get("rightCol");
		Dataset<Row> purgedRightDS = getPurgedDS(rightInput, rightSet, rightCol);
//		purgedRightDS.show(1);

		List<Tuple2<String, Dataset<Row>>> rightRows = Colls.list(new Tuple2<>(rightTableName, purgedRightDS));

		List<List<Tuple2<String, Dataset<Row>>>> lll = Colls.list(leftRows, left -> Colls.list(rightRows, //
				right -> doJoin(left, right, (String) ctx.get("leftCol"), (String) ctx.get("rightCol"), //
						(String) ctx.get("type"), (String) ctx.get("as"),leftTableName,rightTableName)));
		return Colls.flat(lll);
	}

	private Dataset<Row> getPurgedDS(SparkInput<Rmap> input, Set<String> fields, String joincol) {
			Dataset<Row> ds = input.rows().get(0)._2;
			Set<String> addFields = new HashSet<>();
			String[] columns = ds.columns();
			for (int i = 0; i < columns.length; i++) {
				addFields.add(columns[i]);
			}
//			不drop碰撞字段
			addFields.remove(joincol);
			// 存放非展示字段
			Set<String> resultSet = new HashSet<>();
			resultSet.clear();
			resultSet.addAll(addFields);
			resultSet.removeAll(fields);
			Dataset<Row> purgeDS = purge(ds, resultSet);
			return purgeDS;
	}

	private Dataset<Row> purge(Dataset<Row> ds, Set<String> fields_list) {
		Dataset<Row> d = ds;
		for (String f : fields_list)
			if (ds.schema().getFieldIndex(f).nonEmpty()) d = d.drop(ds.col(f));
		return d;
	}

//	需要传入旧的ds clume,新的column,然后去遍历旧的ds,
	private Dataset<Row> renameDS(Dataset<Row> ds, String tableName) {
		Dataset<Row> d = ds;
		String[] columns = ds.columns();
		for (String oldCol : columns)
			 d = d.withColumnRenamed(oldCol,addPrefix(tableName,oldCol));
		return d;
	}

	private String addPrefix(String tableName,String oldC) {
		String resultName = tableName + "_" + oldC;
		return resultName;
	}


	public Tuple2<String, Dataset<Row>> doJoin(Tuple2<String, Dataset<Row>> ids, Tuple2<String, Dataset<Row>> jds, String ic, String jc,
											   String type, String asTable,String leftName, String rightName) {
		Dataset<Row> main = ids._2;
		Dataset<Row> sub = jds._2;
		String joinName = asTable; // ids._1 + "*" + jds._1;
//		如果在最后处理
		Dataset<Row> ds = main.join(sub, main.col(ic).equalTo(sub.col(jc)), type).distinct();
		return new Tuple2<>(joinName, ds);
	}

	@Override
	public void close() {
		super.close();
		input.close();
		joined.close();
	}
}
