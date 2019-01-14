package net.butfly.albatis.mongodb;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.spark.MongoSpark;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.impl.SparkIO.Schema;
import net.butfly.albatis.spark.output.SparkSinkSaveOutput;
import net.butfly.albatis.spark.output.SparkWriting;
import net.butfly.albatis.spark.util.DSdream;

@Schema("mongodb")
public class SparkMongoOutput extends SparkSinkSaveOutput implements SparkWriting, SparkMongo {
	private static final long serialVersionUID = -887072515139730517L;
	private static final int BATCH_SIZE = 250;
	private final String mongodbn;
	private final MongoSpark mongo;

	public SparkMongoOutput(SparkSession spark, URISpec targetUri, TableDesc... table) {
		super(spark, targetUri, table);
		mongodbn = targetUri.getPathAt(0);
		mongo = MongoSpark.builder().options(options(null)).javaSparkContext(new JavaSparkContext(spark.sparkContext())).build();
	}

	@Override
	public Map<String, String> options(String table) {
		Map<String, String> opts = mongoOpts(targetUri);
		opts.put("replaceDocument", "true");
		opts.put("maxBatchSize", Integer.toString(BATCH_SIZE));
		opts.put("localThreshold", "0");
		opts.put("writeConcern.w", "majority");
		opts.put("collection", "spark");
		if (null != table) opts.put("collection", table);
		return opts;
	}

	@Override
	public boolean writing(long partitionId, long version) {
		logger().error("MongoDB writing, should init res here");
		return true;
	}

	@Override
	public void enqueue(Sdream<Rmap> s) {
		if (s instanceof DSdream) write(((DSdream) s).table, ((DSdream) s).list());
		else {
			Map<String, BlockingQueue<Rmap>> m = Maps.ofQ(s, Rmap::table);
			for (String t : m.keySet())
				write(t, m.get(t));
		}
	}

	// protected void write(Row row) {
	// Rmap r = row2rmap(row);
	// long rr = write(mongo.connector().acquireClient().getDatabase(mongodbn).getCollection(r.table()), r);
	// if (rr > 0) succeeded(rr);
	// else failed(Sdream.of());
	// }

	private void write(String t, Collection<Rmap> docs) {
		LinkedBlockingQueue<Rmap> l;
		if (docs instanceof BlockingQueue) l = (LinkedBlockingQueue<Rmap>) docs;
		else l = new LinkedBlockingQueue<>(docs);

		long total = l.size();
		logger().trace("MongoSpark upsert [" + total + "] to [" + t + "] with batch [" + BATCH_SIZE + "] starting.");
		long now = System.currentTimeMillis();
		MongoCollection<Document> col = mongo.connector().acquireClient().getDatabase(mongodbn).getCollection(t);
		AtomicLong succ = new AtomicLong();
		List<Rmap> fails = Colls.list();
		while (!l.isEmpty()) {
			List<Rmap> batch = Colls.list();
			l.drainTo(batch, BATCH_SIZE);
			if (!batch.isEmpty()) succ.addAndGet(write(col, batch));
		}
		logger().trace("MongoSpark upsert [" + total + "] to [" + t + "] with batch [" + BATCH_SIZE + "] finished, successed [" + succ.get()
				+ "], failed [" + fails.size() + "], spent: " + now / 1000 + " ms.");
	}

	private int write(MongoCollection<Document> col, List<Rmap> rmapList) {
		AtomicInteger atomicInteger = new AtomicInteger();
		// 调用statsOuts方法,传入list,和一个函数.
		s().statsOuts(rmapList, rs -> {
			List<WriteModel<Document>> ws = Colls.list(rs, this::write);
			// 调用MOngoCollection的bulkWrite方法,传入上边的List
			BulkWriteResult result = col.bulkWrite(ws);
			// 调用result的Count,求和
			int cc = result.getInsertedCount() + (result.isModifiedCountAvailable() ? result.getModifiedCount() : 0) + result.getUpserts()
					.size();
			atomicInteger.addAndGet(cc);
		});
		return atomicInteger.get();
	}

	// 传入一个rmap，返回一个WriteModel
	protected WriteModel<Document> write(Rmap rmap) {
		// 传入rmap创建doc对象
		Document doc = new Document(rmap);
		// 如果rmap的key是空并且rmap不包含_id,就返回InsertOneModel对象,同时传入上一步的doc对象
		if (null == rmap.keyField() && !rmap.containsKey("_id")) return new InsertOneModel<Document>(doc);
		Document newDoc = null;
		// 如果rmap的key属性不空,就传入keyfield和key用来创建doc对象.否则,如果rmap包含_id,就创建doc对象,只传入_id的映射
		if (null != rmap.keyField()) newDoc = new Document(rmap.keyField(), rmap.key());
		else if (rmap.containsKey("_id")) newDoc = new Document("_id", rmap.get("_id"));
		// 返回replaceOneModel对象
		return new ReplaceOneModel<>(newDoc, doc, new ReplaceOptions().upsert(true));
	}

	// 传入一个MongoC对象,一个rmap
	protected long write(MongoCollection<Document> col, Rmap rmap) {
		Document doc = new Document(rmap);
		Document docResult = null;
		if (null != rmap.keyField()) docResult = new Document(rmap.keyField(), rmap.key());
		else if (rmap.containsKey("_id")) docResult = new Document("_id", rmap.get("_id"));
		Document doc2 = docResult;
		AtomicLong atom = new AtomicLong();
		s().statsOut(rmap, rr -> {
			try {
				if (null != doc2) {
					ReplaceOptions upsert = new ReplaceOptions().upsert(true);
					UpdateResult u = col.replaceOne(doc2, doc, upsert);
					// UpdateResult updateResult = col.replaceOne(doc2, doc, new ReplaceOptions().upsert(true));
					atom.set(u.getModifiedCount());
				} else {
					col.insertOne(doc);
					atom.set(1);
				}
			} catch (Exception ex) {
				logger().debug("MongoSpark fail: " + rmap, ex);
				atom.set(-1);
			}
		});
		return atom.get();
	}
}
