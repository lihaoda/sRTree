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
class RTreePartitioner(recs: Array[MBR], maxEntryPerNode:Int) extends Partitioner {

  println(s"Partition num: ${recs.length}")

  def mbrs = recs

  val tree = RTree(recs.zipWithIndex.map(x => (x._1, x._2, 1)), maxEntryPerNode)

  def noNegMod(x:Int, mod:Int):Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  override def numPartitions: Int = recs.length

  override def getPartition(key: Any): Int = key match {
    case g: Point =>
      tree.circleRange(g, 0.0).head._2
    case _ =>
      noNegMod(key.hashCode, numPartitions)
  }


  override def equals(other: Any): Boolean = other match {
    case r: RTreePartitioner =>
      mbrs.zip(r.mbrs).forall(a => a._1.equals(a._2))
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
  val defaultGlobalMaxEntries = 4
  def create(sampleData:Array[Point], approximateNumPartitions:Int, bound:MBR, globalMaxEntryPerNode:Int = defaultGlobalMaxEntries): RTreePartitioner = {
    val recs = RTree.divideMBR(sampleData, approximateNumPartitions, bound)
    new RTreePartitioner(recs, globalMaxEntryPerNode)
  }
}


