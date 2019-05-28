package net.butfly.albatis.mongodb;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
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
import net.butfly.albatis.spark.impl.Schemas;
import net.butfly.albatis.spark.impl.SparkIO.Schema;
import net.butfly.albatis.spark.output.SparkSinkSaveOutput;
import net.butfly.albatis.spark.output.SparkWriting;
import net.butfly.albatis.spark.util.DSdream;

@Schema("mongodb")
public class SparkMongoOutput extends SparkSinkSaveOutput implements SparkWriting, SparkMongo {
	private static final long serialVersionUID = -887072515139730517L;
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
		opts.put("maxBatchSize", Integer.toString(batchSize()));
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
		if (s instanceof DSdream) {
			long start = System.currentTimeMillis();
			((DSdream) s).ds.repartition(2000).foreachPartition((ForeachPartitionFunction<Row>) rows -> write(((DSdream) s).table, Colls.list(rows, Schemas::row2rmap))); //TODO add time monitor
			logger().info("foreachPartition use:\t"+ (System.currentTimeMillis()-start)/1000 + "s" );
		} else {
			Map<String, BlockingQueue<Rmap>> m = Maps.ofQ(s, Rmap::table);
			for (String t : m.keySet())
				write(t, m.get(t));
		} ;
	}

	private void write(String table, Collection<Rmap> docs) {
		if (Colls.empty(docs)) return;
		LinkedBlockingQueue<Rmap> l;
		if (docs instanceof BlockingQueue) l = (LinkedBlockingQueue<Rmap>) docs;
		else l = new LinkedBlockingQueue<>(docs);

		MongoCollection<Document> col = mongo.connector().acquireClient().getDatabase(mongodbn).getCollection(table);
		AtomicLong succ = new AtomicLong();
		while (!l.isEmpty()) {
			List<Rmap> batch = Colls.list();
			l.drainTo(batch, batchSize());
			if (!batch.isEmpty()) succ.addAndGet(write(col, batch));
		}
	}

	private long write(MongoCollection<Document> col, List<Rmap> rmapList) {
		AtomicLong c = new AtomicLong();
		s().statsOuts(rmapList, rs -> {
			List<WriteModel<Document>> ws = Colls.list(rs, this::write);
			long now = System.currentTimeMillis();
			int cc = 0;
			try {
				BulkWriteResult result = col.bulkWrite(ws);
				cc = result.isModifiedCountAvailable() ? result.getModifiedCount() : 0;
				cc = result.getInsertedCount() + cc + result.getUpserts().size();
				c.addAndGet(cc);
			} finally {
				long spent = System.currentTimeMillis() - now;
				long ccc = cc;
				logger().trace(() -> "MongoSpark upsert [" + ws.size() + "] to [" + col + "] with batch [" + batchSize()
						+ "] finished, successed [" + ccc + "], spent: " + spent + " ms.");
			}
		});
		return c.get();
	}

	protected WriteModel<Document> write(Rmap rmap) {
		Document doc = new Document(rmap);
		if (null == rmap.keyField() && !rmap.containsKey("_id")) return new InsertOneModel<Document>(doc);
		Document newDoc = null;
		if (null != rmap.keyField()) newDoc = new Document(rmap.keyField(), rmap.key());
		else if (rmap.containsKey("_id")) newDoc = new Document("_id", rmap.get("_id"));
		// 返回replaceOneModel对象
		return new ReplaceOneModel<>(newDoc, doc, new ReplaceOptions().upsert(true));
	}

	protected long write(MongoCollection<Document> col, Rmap rmap) {
		Document doc = new Document(rmap);
		Document docResult = null;
		if (null != rmap.keyField()) docResult = new Document(rmap.keyField(), rmap.key());
		else if (rmap.containsKey("_id")) docResult = new Document("_id", rmap.get("_id"));
		Document doc2 = docResult;
		AtomicLong atom = new AtomicLong();
		s().statsOutN(rmap, rr -> {
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
