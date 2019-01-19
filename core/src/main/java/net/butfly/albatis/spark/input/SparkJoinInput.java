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
		Set<String> leftSet = (Set<String>) ctx.get("leftSet");
		String leftjoinCol = (String) ctx.get("leftCol");
		// 拿到input的表名
		String tableName = leftInput.targetUri.getFile();
		Dataset<Row> purgedLeftDS = getPurgedDS(leftInput, leftSet, leftjoinCol);

//        purgedLeftDS.Tables[0].Columns.Remove("Username")

		List<Tuple2<String, Dataset<Row>>> leftRows = Colls.list(new Tuple2<>(tableName, purgedLeftDS));
//		purgedLeftDS.show(1);
//       todo 给leftds加入左边_字段的别名
//        String[] columns = purgedLeftDS.columns();
//        for (int i = 0; i < columns.length; i++) {
//            addFields.add(columns[i]);
//        }


        SparkInput<Rmap> rightInput = (SparkInput<Rmap>) ctx.get("rightInput");
		Set<String> rightSet = (Set<String>) ctx.get("rightSet");
		String rightCol = (String) ctx.get("rightCol");
		Dataset<Row> purgedRightDS = getPurgedDS(rightInput, rightSet, rightCol);
//		purgedRightDS.show(1);
		String rightName = rightInput.targetUri.getFile();
		List<Tuple2<String, Dataset<Row>>> rightRows = Colls.list(new Tuple2<>(rightName, purgedRightDS));

		List<List<Tuple2<String, Dataset<Row>>>> lll = Colls.list(leftRows, left -> Colls.list(rightRows, //
				right -> doJoin(left, right, (String) ctx.get("leftCol"), (String) ctx.get("rightCol"), //
						(String) ctx.get("type"), (String) ctx.get("as"))));
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

	public Tuple2<String, Dataset<Row>> doJoin(Tuple2<String, Dataset<Row>> ids, Tuple2<String, Dataset<Row>> jds, String ic, String jc,
			String type, String asTable) {
		Dataset<Row> main = ids._2;
		Dataset<Row> sub = jds._2;
		String joinName = asTable; // ids._1 + "*" + jds._1;
		Dataset<Row> ds = main.join(sub, main.col(ic).equalTo(sub.col(jc)), type).distinct();
//		ds.show(1);
		return new Tuple2<>(joinName, ds);
	}

	@Override
	public void close() {
		super.close();
		input.close();
		joined.close();
	}
}
