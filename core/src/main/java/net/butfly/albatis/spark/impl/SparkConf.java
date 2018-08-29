package net.butfly.albatis.spark.impl;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import net.butfly.albatis.spark.impl.SparkConf.SparkConfItems;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Repeatable(SparkConfItems.class)
public @interface SparkConf {
	/**
	 * @return key=value without key attribute definition<br>
	 *         or value with key attribute definition.
	 */
	String value();

	String key() default "";

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.TYPE)
	@interface SparkConfItems {
		SparkConf[] value();
	}
}
