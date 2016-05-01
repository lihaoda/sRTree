/**
  * Created by lihaoda on 2016/3/28.
  */

package edu.thu.cs.database.benchmark

import com.github.davidmoten.rtree.RTree
import com.github.davidmoten.rtree.geometry._
import org.apache.spark._
import edu.thu.cs.database.spark.rdd.RTreeRDD
import edu.thu.cs.database.spark.rdd.RTreeRDD._

object DataTest {

  def main(args: Array[String]): Unit = {
    println("Hello, world!")
    if(args.length < 1) {
      println("missing arguments: data file path")
    }
    val conf = new SparkConf()
      .setAppName("RTreeRddTest")
    val sc = new SparkContext(conf)
    val data = sc.textFile(args(0)).map ( s => {
      val strs = s.split(" ")
      (Geometries.point(strs(1).toDouble, strs(2).toDouble), strs(0).toInt)
    })

    data.cache()
    val single = data.buildRTree(1)
    val multi = data.buildRTree(50)
    single.saveAsRTreeFile("/home/spark/test_data/small_saved.txt")
    val rec = Geometries.rectangle(40,115,41,116)
    val singleResult = single.search(rec)
    val multiResult = multi.search(rec)
    val joinResult = singleResult.join(multiResult)
    println(s"singleResult count: ${singleResult.count()}")
    println(s"multiResult count: ${multiResult.count()}")
    println(s"joinResult count: ${joinResult.count()}")
    joinResult.takeSample(false, 10).foreach(println)
  }
}
