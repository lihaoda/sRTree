/**
  * Created by lihaoda on 2016/3/28.
  */

package edu.thu.cs.database.benchmark

import java.io.{ByteArrayInputStream, ObjectInputStream, ObjectOutputStream, ByteArrayOutputStream}

import com.github.davidmoten.guavamini.Sets
import com.github.davidmoten.rtree.{Entries, RTree, Entry}
import com.github.davidmoten.rtree.geometry._
import org.apache.spark._
import edu.thu.cs.database.spark.rdd.RTreeRDD
import java.nio.ByteBuffer
import edu.thu.cs.database.spark.rdd.RTreeRDD._

object DataTest {

  def rtreeTest() = {
    val a: Entry[String, Point] = Entries.entry("p1", Geometries.point(1, 2))
    val b: Entry[String, Point] = Entries.entry("p2", Geometries.point(3, 4))
    val c: Entry[String, Point] = Entries.entry("p3", Geometries.point(5, 6))
    var tree: RTree[String, Point] = RTree.create[String, Point]
    tree = tree.add(a).add(b)

    val bytes: ByteArrayOutputStream = new ByteArrayOutputStream
    val out: ObjectOutputStream = new ObjectOutputStream(bytes)

    out.writeObject(tree)
    out.close


    val in: ObjectInputStream = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray))
    var tree2: RTree[String, Point] = null
    try {
      tree2 = in.readObject.asInstanceOf[RTree[String, Point]]
      println(tree2.mbr.isPresent)
      if (tree2.mbr.isPresent) {
        println(tree2.mbr.get)
      }
    }
    catch {
      case e: ClassNotFoundException => {
        e.printStackTrace
      }
    }

    tree2 = tree2.add(c)
    require(Sets.newHashSet(a, b, c).equals(Sets.newHashSet(tree2.entries.toList.toBlocking.single)))
  }

  def main(args: Array[String]): Unit = {
    rtreeTest();
    /*
    println("Hello, world!")
    if(args.length < 1) {
      println("missing arguments: data file path")
    }
    val conf = new SparkConf()
      .setAppName("RTreeRddTest")
    val sc = new SparkContext(conf)
    val data = sc.textFile(args(0)).map ( s => {
      val strs = s.split(" ")
      (Geometries.point(strs(1).toDouble, strs(2).toDouble), strs(0).toInt.toString)
    })

    data.cache()
    val single = data.buildRTree(1)
    val multi = data.buildRTree(50)
    single.saveAsRTreeFile("/home/spark/test_data/small_saved.txt",
      a => a.getBytes,
      b => String.valueOf(b))
    val rec = Geometries.rectangle(40,115,41,116)
    val singleResult = single.search(rec)
    val multiResult = multi.search(rec)
    val joinResult = singleResult.join(multiResult)
    println(s"singleResult count: ${singleResult.count()}")
    println(s"multiResult count: ${multiResult.count()}")
    println(s"joinResult count: ${joinResult.count()}")
    joinResult.takeSample(false, 10).foreach(println)
    */
  }
}
