# SPARK集成

## spark-integrate

### 定义

- group: com.hzcominfo.dataggr.spark.integrate
- artifact: spark-integrate
- version: 0.0.1-SNAPSHOT

#### 部件

- spark-integrate-core：核心依赖包，创建spark会话、关闭会话等
- 集成模块：
	- spark-integrate-kafka：kafka输入、输出
	- spark-integrate-mongo：mongo输入，输出
	- spark-integrate-rdbms：rdbms输入，输出
	- spark-integrate-hdfs: hdfs输入，输出
	

#### 代码
1. common demo
	~~~java
		logger.info("Calcing : [Featuretest]");
		URISpec iu = new URISpec("kafka://data01:9092,data02:9092,data03:9092/spark_test");
		URISpec ou = new URISpec("mongodb://devdb:Devdb1234@172.30.10.31:40012/devdb/SPARK_FEATURE_TEST");
		try (SparkConnection sc = new SparkConnection("FeatureSpark", iu);
				SparkInput in = sc.input(iu);
				SparkOutput out = sc.output(ou);
				Pump<Row> p = in.pump(out)) {
			p.open();
			System.out.println("starting...");
		} catch (Throwable t) {
			logger.error("failed", t);
		}
		System.out.println("end...");
	~~~

1. collision demo
	~~~java
		logger.info("Calcing : [CollisionTest]");
		URISpec kiu = new URISpec("kafka:etl://data01:9092,data02:9092,data03:9092/ZHW_TLGA_GNSJ_NEW_1_1");
		URISpec miu = new URISpec("mongodb://devdb:Devdb1234@172.30.10.31:40012/devdb/SPARK_FEATURE_SUB");
		URISpec ou = new URISpec("mongodb://devdb:Devdb1234@172.30.10.31:40012/devdb/SPARK_FEATURE_TEST");

		try (SparkConnection sc = new SparkConnection("collision-apptest", kiu);
				SparkMongoInput min = sc.input(miu);
				SparkKafkaEtlInput kin = sc.input(kiu);
				SparkJoinInput jin = sc.innerJoin(kin, "CERTIFIED_ID", new InputMapTool().append(min, "ZJHM").get());
				SparkOutput out = sc.output(ou);
				Pump<Row> p = jin.pump(out);) {
			kin.schema(FeatureTest.SAMPLE_SCHEMA);
			
			p.open();
			System.out.println("starting...");
		} catch (Throwable t) {
			logger.error("failed", t);
		} finally {
		}
		System.out.println("end...");
	~~~
	
1. plugin demo
	~~~java
		logger.info("Calcing : [InvokeTest]");

		URISpec kiu = new URISpec("kafka:etl://data01:9092,data02:9092,data03:9092/ZHW_TLGA_GNSJ_NEW_1_1");
		String className = "com.hzcominfo.dataggr.spark.plugin.SparkCommonPluginInput";

		List<String> keys = new ArrayList<>();
		keys.add("CERTIFIED_ID");
		try (SparkConnection sc = new SparkConnection("invoke-apptest", kiu);
				SparkKafkaEtlInput kin = sc.input(kiu);
				SparkInput in = sc.plugin(className, kin,
						new PluginConfig(keys, "TRAIN_DAY"));
				FeatureOutput out = new FeatureOutput();
				Pump<Map<String, Object>> p = in.pump(out, new HashMap<>())) {
			kin.schema(FeatureTest.SAMPLE_SCHEMA);

			p.open();
			System.out.println("starting...");
		} catch (Throwable t) {
			logger.error("failed", t);
		} finally {
		}
		System.out.println("end...");
	~~~
	
#### 说明
1. 支持不同数据源自定义流向 
2. 支持多表碰撞（join, multi-join,left-anti join）
3. 支持插件配置的统计输出

#### 备注 
1. 使用kafka:etl的schema连接时需要设置schema