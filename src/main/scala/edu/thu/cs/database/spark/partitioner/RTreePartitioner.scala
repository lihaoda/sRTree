package edu.thu.cs.database.spark.partitioner

import java.io.{ObjectInputStream, ObjectOutputStream, IOException}

import edu.thu.cs.database.spark.spatial._
import edu.thu.cs.database.spark.rtree._
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.{SparkEnv, Partitioner}

import org.apache.spark.util.Utils

import scala.reflect.ClassTag

/**
  * Created by lihaoda on 16-4-21.
  */
class RTreePartitioner(var recs: Array[MBR]) extends Partitioner {

  println(s"Partition num: ${recs.length}")

  def noNegMod(x:Int, mod:Int):Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  override def numPartitions: Int = recs.length + 1
  override def getPartition(key: Any): Int = key match {
    case null => 0
    case g: Point =>
      val cand = recs.zipWithIndex.filter( a => g.intersects(a._1) )
      if(cand.length > 0)
        cand(noNegMod(g.hashCode, cand.length))._2
      else
        recs.length
    case g: MBR =>
      val cand = recs.zipWithIndex.filter( a => g.intersects(a._1) )
      if(cand.length > 0)
        cand(noNegMod(g.hashCode, cand.length))._2
      else
        recs.length
    case _ => noNegMod(key.hashCode, numPartitions)
  }


  override def equals(other: Any): Boolean = other match {
    case r: RTreePartitioner =>
      recs.zip(r.recs).forall(a => a._1.equals(a._2))
    case _ =>
      false
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    recs.foreach( a => {
      result = prime*result + a.hashCode
    })
    result
  }
  /*
  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => out.defaultWriteObject()
      case _ =>
        out.writeObject(recs)
    }
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => in.defaultReadObject()
      case _ =>
        recs = in.readObject().asInstanceOf[Array[Rectangle]]
    }
  }
  */
}

object RTreePartitioner {
  var useLeaf = true
  def getRTreeRecs(sampleData:Array[Point], recNum:Int):Array[MBR] = {
    //
    if(useLeaf) {
      val entryPernode = sampleData.length / recNum
      RTree(sampleData.map((_, 1)), entryPernode).leafMBR()
    } else {
      RTree(sampleData.map((_, 1)), 2).divideMBR(recNum)
    }
  }
  def create(sampleData:Array[Point], numPartitions:Int): RTreePartitioner = {
    val recs = getRTreeRecs(sampleData, numPartitions-1);
    //recs.foreach(println)
    new RTreePartitioner(recs)
  }
}


