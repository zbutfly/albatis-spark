package com.hzcominfo.search.collision.util;

public class Strs {

	public static String char_concat(String c, String... strs) {
		if (strs == null)
			return null;
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < strs.length; i++) {
			if (null != strs[i])
				sb.append(strs[i]);
			if (i < strs.length - 1)
				sb.append(c);
		}
		return sb.toString();
	}

	public static String concat(String... strs) {
		if (strs == null)
			return null;
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < strs.length; i++) {
			if (null != strs[i])
				sb.append(strs[i]);
		}
		return sb.toString();
	}

	public static String split_part(String str, String c, int part) {
		if (null == str || null == c)
			return null;
		String[] split = str.split(c);
		if (split.length < part)
			return null;
		return split[part - 1];
	}
}
