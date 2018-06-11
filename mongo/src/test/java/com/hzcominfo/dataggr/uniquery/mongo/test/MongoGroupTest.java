package com.hzcominfo.dataggr.uniquery.mongo.test;

import com.google.gson.JsonObject;
import com.hzcominfo.dataggr.uniquery.SqlExplainer;
import com.hzcominfo.dataggr.uniquery.mongo.MongoQuery;
import com.hzcominfo.dataggr.uniquery.mongo.MongoQueryVisitor;
import com.mongodb.*;
import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.mongodb.MongoConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class MongoGroupTest {

    private static final String uri = "mongodb://yjdb:yjdb1234@172.30.10.101:22001/yjdb/GROUP_TEST_COLLECTION?readPreference=primary";
    private MongoConnection connection;
    private DBCollection collection;

    @Before
    public void b() throws IOException {
        connection = new MongoConnection(new URISpec(uri));
        collection = connection.collection();
    }

    @After
    public void a() {
        if (null != connection) connection.close();
    }

    @Test
    public void g1() {
        DBObject group =  new BasicDBObject();
        DBObject gf = new BasicDBObject();
        gf.put("XB", "$XB");
//        gf.put("MZ", "$MZ");

        group.put("_id", gf);
        group.put("MIN(NL)", new BasicDBObject("$min", "$NL"));
        group.put("MAX(NL)", new BasicDBObject("$max", "$NL"));
        group.put("COUNT", new BasicDBObject("$sum", 1));
//        DBObject where = QueryBuilder.start("NL").greaterThan(20).get();
        DBObject where = QueryBuilder.start("NL").exists(true).get();

        List<DBObject> pipeline = Arrays.asList(
                new BasicDBObject("$match", where)
                , new BasicDBObject("$group", group)
        );
        Cursor cursor = collection.aggregate(pipeline, AggregationOptions.builder().allowDiskUse(true).build());
        AtomicInteger idx = new AtomicInteger();
        cursor.forEachRemaining(o -> System.out.println(idx.incrementAndGet() + "---->" + o));
    }

    @Test
    public void g2() {
        DBObject group =  new BasicDBObject();
        DBObject gf = new BasicDBObject();
        gf.put("XB", "$XB");
        gf.put("MZ", "$MZ");
        group.put("_id", gf);
        group.put("MIN(NL)", new BasicDBObject("$min", "$NL"));
        group.put("MAX(NL)", new BasicDBObject("$max", "$NL"));
        DBObject where = QueryBuilder.start("NL").greaterThan(20).get();
        List<DBObject> pipeline = Arrays.asList(
                new BasicDBObject("$match", where),
                new BasicDBObject("$group", group)
        );
        Cursor cursor = collection.aggregate(pipeline, AggregationOptions.builder().allowDiskUse(true).build());
        AtomicInteger idx = new AtomicInteger();
        cursor.forEachRemaining(o -> System.out.println(idx.incrementAndGet() + "---->" + o));
    }

    @Test
    public void g3() {
        String sql = "SELECT MIN(NL), MAX(NL) as max_nl, COUNT(*) as cnt FROM GROUP_TEST_COLLECTION GROUP BY XB, MZ";
        JsonObject json = SqlExplainer.explain(sql);
        System.out.println(json);
        MongoQuery mq = new MongoQueryVisitor(new MongoQuery(), json).get();
        List<DBObject> pipeline = mq.getPipeline();
        Cursor cursor = collection.aggregate(pipeline, AggregationOptions.builder().allowDiskUse(true).build());
        AtomicInteger idx = new AtomicInteger();
        cursor.forEachRemaining(o -> System.out.println(idx.incrementAndGet() + "---->" + o));
    }
}
