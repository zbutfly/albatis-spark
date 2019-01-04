package net.butfly.albatis.elastic;

public class UriTest {

	public static void main(String[] args) {
//		String uri = "es://hzcominfo@172.30.10.31:39300/test_phga_search?httpport=39200";
		 String uri = "zk://data01:7181,data02:7181,data03:7181";
		String table = "_type";
		String u = uri.contains("?")
				? Strs.concat(Strs.split_part(uri, "\\?", 1), "/", table, "?", Strs.split_part(uri, "\\?", 2))
				: Strs.concat(uri, "/", table);
		System.out.println(u);
	}
}

class Strs {

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
