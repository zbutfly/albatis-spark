package net.butfly.albatis.mongodb;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
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

@Schema("mongodb")
public class SparkMongoOutput extends SparkSinkSaveOutput implements SparkWriting, SparkMongo {
	private static final long serialVersionUID = -887072515139730517L;
	private static final int BATCH_SIZE = 250;
	private final String mongodbn;
	private final MongoSpark mongo;

	public SparkMongoOutput(SparkSession spark, URISpec targetUri, TableDesc... table) {
		super(spark, targetUri, table);
		mongodbn = targetUri.getPathAt(0);
		mongo = MongoSpark.builder().options(options()).javaSparkContext(new JavaSparkContext(spark.sparkContext())).build();
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
		logger().error("MongoDB writing, should init res here");
		return true;
	}

	@Override
	public void enqueue(Sdream<Rmap> s) {
		Maps.ofQ(s, Rmap::table).forEach(this::write);
	}

	// protected void write(Row row) {
	// Rmap r = row2rmap(row);
	// long rr = write(mongo.connector().acquireClient().getDatabase(mongodbn).getCollection(r.table()), r);
	// if (rr > 0) succeeded(rr);
	// else failed(Sdream.of());
	// }

	private void write(String t, BlockingQueue<Rmap> docs) {
		Map<String, String> opts = options();
		opts.put("collection", t);
		long total = docs.size();
		logger().trace("MongoSpark upsert [" + total + "] to [" + t + "] with batch [" + BATCH_SIZE + "] starting.");
		long now = System.currentTimeMillis();
		MongoCollection<Document> col = mongo.connector().acquireClient().getDatabase(mongodbn).getCollection(t);
		AtomicLong succ = new AtomicLong();
		List<Rmap> fails = Colls.list();
		while (!docs.isEmpty()) {
			List<Rmap> batch = Colls.list();
			docs.drainTo(batch, BATCH_SIZE);
			if (!batch.isEmpty()) succ.addAndGet(write(col, batch));
		}
		logger().trace("MongoSpark upsert [" + total + "] to [" + t + "] with batch [" + BATCH_SIZE + "] finished, successed [" + succ.get()
				+ "], failed [" + fails.size() + "], spent: " + now / 1000 + " ms.");
	}

	private int write(MongoCollection<Document> col, List<Rmap> l) {
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

	protected long write(MongoCollection<Document> col, Rmap r) {
		Document d = new Document(r);
		Document q = null;
		if (null != r.keyField()) q = new Document(r.keyField(), r.key());
		else if (r.containsKey("_id")) q = new Document("_id", r.get("_id"));
		Document qq = q;
		AtomicLong l = new AtomicLong();
		s().statsOut(r, rr -> {
			try {
				if (null != qq) {
					UpdateResult u = col.replaceOne(qq, d, new ReplaceOptions().upsert(true));
					l.set(u.getModifiedCount());
				} else {
					col.insertOne(d);
					l.set(1);
				}
			} catch (Exception ex) {
				logger().debug("MongoSpark fail: " + r, ex);
				l.set(-1);
			}
		});
		return l.get();
	}
}
