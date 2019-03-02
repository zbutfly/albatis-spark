package net.butfly.albatis.spark;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.pump.Pump;
import net.butfly.albatis.spark.output.SparkSinkOutput;

import org.apache.livy.*;

public class SparkPump extends Namedly implements Pump<Rmap>, Serializable {
	private static final long serialVersionUID = -6842560101323305087L;
	private final SparkInput<Rmap> input;
	private final SparkOutput<Rmap> output;

	SparkPump(SparkInput<Rmap> input, Output<Rmap> dest) {
		super(input.name() + ">" + dest.name());
		this.input = input;
		this.output = dest instanceof SparkOutput ? (SparkOutput<Rmap>) dest : new SparkSinkOutput(input.spark, dest);
		Reflections.noneNull("Pump source/destination should not be null", input, dest);
	}

	@Override
	public void open() {
		output.open();
		input.open();
		Pump.super.open();
		(input.rows.isEmpty() ? output.compute(input.vals) : input.rows).forEach(t -> output.save(t._1, t._2));
		close();

		Map<String,String> map = Maps.of();
		map.put("kind", "spark");
		map.put("name", "0301test");
		try {
			LivyClientBuilder builder = new LivyClientBuilder();
			builder.setAll(map);
			builder.setConf("spark.cores.max", "4");
			builder.setURI(new URI("http://172.30.10.31:8998/sessions/"));
			LivyClient client  = builder.build();
//			client.submit()
		} catch (IOException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() {
		input.close();
		Pump.super.close();
		output.close();
	}
}
