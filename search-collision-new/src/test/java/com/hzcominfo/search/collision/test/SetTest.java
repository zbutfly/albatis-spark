package com.hzcominfo.search.collision.test;

import java.util.HashSet;
import java.util.Set;

import com.hzcominfo.search.collision.util.Strs;

public class SetTest {

	public static void main(String[] args) {
		Set<String> set = new HashSet<>();
		set.add("1");
		set.add("2");
		set.add("3");
		set.stream().forEach(t -> Strs.concat(t, " as ", Strs.char_concat(".", "0", t)));
		System.out.println(set.toString());
	}
}
