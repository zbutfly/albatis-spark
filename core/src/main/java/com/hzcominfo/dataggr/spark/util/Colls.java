package com.hzcominfo.dataggr.spark.util;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class Colls {

	static <E> List<E> list() {
		return new CopyOnWriteArrayList<>();
	}
	
	@SafeVarargs
	public static <E> List<E> list(E... eles) {
		if (null == eles || eles.length == 0) return list();
		List<E> l = list();
		for (E e : eles)
			if (null != e) l.add(e);
		return l;
	}
}
