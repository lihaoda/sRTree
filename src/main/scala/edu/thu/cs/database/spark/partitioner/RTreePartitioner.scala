package edu.thu.cs.database.spark.partitioner

import java.io.{ObjectInputStream, ObjectOutputStream, IOException}

import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.{SparkEnv, Partitioner}

import com.github.davidmoten.rtree.geometry.{Geometry, Rectangle}
import org.apache.spark.util.Utils

import scala.reflect.ClassTag

/**
  * Created by lihaoda on 16-4-21.
  */
class RTreePartitioner(var recs: Array[Rectangle]) extends Partitioner {

  def noNegMod(x:Int, mod:Int):Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  override def numPartitions: Int = recs.length + 1
  override def getPartition(key: Any): Int = key match {
    case null => 0
    case g: Geometry =>
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

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => out.defaultWriteObject()
      case _ =>
        out.writeObject(recs)

        val ser = sfactory.newInstance()
        Utils.serializeViaNestedStream(out, ser) { stream =>
          stream.writeObject(scala.reflect.classTag[Array[K]])
          stream.writeObject(rangeBounds)
        }
    }
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => in.defaultReadObject()
      case _ =>
        recs = in.readObject().asInstanceOf[Array[Rectangle]]

        val ser = sfactory.newInstance()
        Utils.deserializeViaNestedStream(in, ser) { ds =>
          implicit val classTag = ds.readObject[ClassTag[Array[K]]]()
          rangeBounds = ds.readObject[Array[K]]()
        }
    }
  }
}
