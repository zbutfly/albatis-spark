package net.butfly.albatis.spark.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.TypeFactory;

import de.undercouch.bson4jackson.BsonFactory;

public class BytesUtils {
	private static final JavaType t = TypeFactory.defaultInstance().constructMapType(HashMap.class, String.class,
			Object.class);
	private static final JsonFactory DEFAULT_BSON_FACTORY = new BsonFactory();


	/**
	 * 将对象转换成Byte，注意，对象以及其组合属性应该均为基本属性或者实现序列化的接口
	 *
	 * @param obj
	 *            传入对象
	 * @return byte数组
	 * @throws IOException
	 */
	public static byte[] objToByte(Object obj) throws IOException {
		if (obj == null) {
			return null;
		}
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
		objectOutputStream.writeObject(obj);
		byte[] bytes = outputStream.toByteArray();
		outputStream.close();
		objectOutputStream.close();
		return bytes;
	}

	/***
	 * 将Byte数组转换成对象 注意可能部分属性为不可序列化导致对象不完全可用
	 *
	 * @param bytes
	 *            传入字符串
	 * @return 对象
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static Object byteToObject(byte[] bytes) throws IOException, ClassNotFoundException {
		if (bytes == null || bytes.length == 0) {
			return null;
		}
		ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
		ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
		Object object = objectInputStream.readObject();
		inputStream.close();
		objectInputStream.close();
		return object;
	}

	public static <T extends Map<String, Object>> T der(byte[] from) {
		if (null == from)
			return null;
		try (ByteArrayInputStream bais = new ByteArrayInputStream(from);) {
			return defaultBsonMapper().readValue(bais, t);
		} catch (IOException e) {
			return null;
		}
	}

	private static ObjectMapper defaultBsonMapper() {
		return standard(new ObjectMapper(DEFAULT_BSON_FACTORY)) //
				.setSerializationInclusion(Include.NON_NULL)//
		;
	}

	private static ObjectMapper standard(ObjectMapper mapper) {
		ObjectMapper m = mapper.enable(Feature.ALLOW_SINGLE_QUOTES)//
				.enable(Feature.IGNORE_UNDEFINED)//
				.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)//
				.disable(MapperFeature.USE_GETTERS_AS_SETTERS)//
				.disable(SerializationFeature.WRITE_NULL_MAP_VALUES)//
				.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)//
		;
		return m.setVisibility(m.getSerializationConfig().getDefaultVisibilityChecker()//
				.withFieldVisibility(Visibility.ANY)//
				.withGetterVisibility(Visibility.NONE)//
				.withSetterVisibility(Visibility.NONE)//
				.withCreatorVisibility(Visibility.NONE)//
		);
	}
}
