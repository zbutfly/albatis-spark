package com.hzcominfo.search.collision.util;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;

import net.butfly.albacore.utils.logger.Logger;

public class JsonUtils {

	static Logger log = Logger.getLogger(JsonUtils.class);

	public static boolean isJson(String json) {
		if (StringUtils.isBlank(json))
			return false;
		try {
			new JsonParser().parse(json);
			return true;
		} catch (JsonParseException e) {
			log.error("bad json: " + json);
			return false;
		}
	}
}
