package com.mxl.education.etl

import org.apache.spark.sql.{Dataset, SparkSession}
import org.junit.Test

class TestApi {

	@Test
	def test01() {
		val spark = SparkSession
			.builder()
			.master("local[2]")
			.appName("ETLApp")
			.getOrCreate()
		val sc = spark.sparkContext

		import spark.implicits._

		val ds: Dataset[(String, Int)] = sc.parallelize(Seq(("Jack", 1), ("Tom", 1), ("Jack", 1), ("Tom", 1), ("Tom", 1), ("Lucy", 1))).toDS()
		ds.cache()
		ds.show()

		ds.groupByKey(_._1).count().show()

		val value: Dataset[List[(String, Int)]] = ds.groupByKey(_._1).mapGroups({ case (key, it) =>
			it.toList
		})

		ds.groupByKey(_._1).mapValues(_._2).count().show()
	}

	@Test
	def test02() {
		println(BigDecimal.apply(10.0435))
	}

}

case class Bean(name: String, v: Int)
