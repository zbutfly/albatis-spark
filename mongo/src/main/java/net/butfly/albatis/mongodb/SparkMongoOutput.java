package net.butfly.albatis.mongodb;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
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
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.impl.SparkIO.Schema;
import net.butfly.albatis.spark.output.SparkSinkSaveOutput;
import net.butfly.albatis.spark.output.SparkWriting;
import net.butfly.albatis.spark.util.DSdream;

@Schema("mongodb")
public class SparkMongoOutput extends SparkSinkSaveOutput implements SparkWriting, SparkMongo {
	private static final long serialVersionUID = -887072515139730517L;
	private static final int BATCH_SIZE = 1000;
	private final String mongodbn;
	private final MongoSpark mongo;

	public SparkMongoOutput(SparkSession spark, URISpec targetUri, String... table) {
		super(spark, targetUri, table);
		mongodbn = targetUri.getPathAt(0);
		mongo = MongoSpark.builder().options(options()).javaSparkContext(new JavaSparkContext(spark.sparkContext())).build();
	}

	@Override
	public void close() {
		super.close();
	}

	@Override
	public Map<String, String> options() {
		Map<String, String> opts = mongoOpts(targetUri);
		opts.put("replaceDocument", "true");
		opts.put("maxBatchSize", Integer.toString(BATCH_SIZE));
		opts.put("localThreshold", "0");
		opts.put("writeConcern", "majority");
		return opts;
	}

	@Override
	public boolean writing(long partitionId, long version) {
		return true;
	}

	@Override
	public void enqueue(Sdream<Rmap> s) {
		if (s instanceof DSdream) write(((DSdream<Rmap>) s).ds);
		else {
			Map<String, BlockingQueue<Rmap>> parts = Maps.of();
			s.eachs(r -> parts.computeIfAbsent(r.table(), t -> new LinkedBlockingQueue<>()).add(r));
			parts.forEach(this::enq);
		}
	}

	private void write(Dataset<Rmap> ds) {
		// JavaRDD<Rmap> rdd = .javaRDD();
		// rdd.JavaPairRDD<String, Iterable<Rmap>> tdd = rdd.groupBy(Rmap::table);
	}

	private void enq(String t, BlockingQueue<Rmap> docs) {
		Map<String, String> opts = options();
		opts.put("collection", t);

		MongoDatabase db = mongo.connector().acquireClient().getDatabase(mongodbn);
		MongoCollection<Document> col = db.getCollection(t);
		AtomicLong succ = new AtomicLong();
		List<Rmap> fails = Colls.list();
		long total = docs.size();
		while (!docs.isEmpty()) {
			List<Rmap> c = Colls.list();
			docs.drainTo(c, BATCH_SIZE);
			if (!c.isEmpty()) succ.addAndGet(write(c, col));
			// docs.forEach(r -> write(r, col, succ, fails));
		}
		// if (succ.get() > 0) succeeded(succ.get());
		// if (!fails.isEmpty()) failed(Sdream.of(fails));
		logger().trace("MongoSpark upsert [" + total + "] with batch [" + BATCH_SIZE + "] finished, successed [" + succ.get()
				+ "], failed [" + fails.size() + "].");
	}

	private int write(List<Rmap> l, MongoCollection<Document> col) {
		AtomicInteger c = new AtomicInteger();
		s().statsOuts(l, rs -> {
			List<WriteModel<Document>> ws = Colls.list(rs, this::write);
			BulkWriteResult w = col.bulkWrite(ws);
			int cc = w.getInsertedCount() + (w.isModifiedCountAvailable() ? w.getModifiedCount() : 0) + w.getUpserts().size();
			c.addAndGet(cc);
		});
		return c.get();
	}

	protected WriteModel<Document> write(Rmap r) {
		Document d = new Document(r);
		if (null == r.keyField() && !r.containsKey("_id")) return new InsertOneModel<Document>(d);
		Document q = null;
		if (null != r.keyField()) q = new Document(r.keyField(), r.key());
		else if (r.containsKey("_id")) q = new Document("_id", r.get("_id"));
		return new ReplaceOneModel<>(q, d, new ReplaceOptions().upsert(true));
	}

	protected void write(Rmap r, MongoCollection<Document> col, AtomicLong succ, List<Rmap> fails) {
		Document d = new Document(r);
		Document q = null;
		if (null != r.keyField()) q = new Document(r.keyField(), r.key());
		else if (r.containsKey("_id")) q = new Document("_id", r.get("_id"));
		try {
			if (null != q) {
				UpdateResult u = col.replaceOne(new Document("_id", r.get("_id")), d, new ReplaceOptions().upsert(true));
				// logger().trace("Upserted: " + u.toString());
				succ.addAndGet(u.getModifiedCount());
			} else {
				col.insertOne(d);
				// logger().trace("Inserted: " + r);
				succ.addAndGet(1);
			}
		} catch (Exception ex) {
			fails.add(r);
			logger().debug("MongoSpark fail: " + r, ex);
		}
	}
}
