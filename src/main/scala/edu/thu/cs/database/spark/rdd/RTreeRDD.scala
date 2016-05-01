package edu.thu.cs.database.spark.rdd

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.github.davidmoten.rtree.geometry.{Geometry, Rectangle}
import com.github.davidmoten.rtree.{InternalStructure, Entry, RTree, Entries}
import edu.thu.cs.database.spark.partitioner.RTreePartitioner
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.rdd.{ShuffledRDD, PartitionPruningRDD, RDD}
import org.apache.spark._

import scala.collection.JavaConversions.asScalaIterator
import scala.reflect.ClassTag

/**
  * Created by lihaoda on 16-3-23.
  */

object RTreeRDD {

  class RTreeRDDImpl[U <: Geometry, T: ClassTag](rdd: RDD[(U, T)]) extends RDD[RTree[T, U]](rdd) {
    override def getPartitions: Array[Partition] = firstParent[(U, T)].partitions
    override def compute(split: Partition, context: TaskContext): Iterator[RTree[T, U]] = {
      val it = firstParent[(U, T)].iterator(split, context)
      var tree = RTree.star().create[T, U]()
      while(it.hasNext) {
        tree = tree.add(it.next())
      }
      Iterator(tree)
    }
  }

  def repartitionRDDorNot[T: ClassTag](rdd: RDD[T], numPartitions: Int): RDD[T] = {
    if (numPartitions > 0) {
      rdd.repartition(numPartitions)
    } else {
      rdd
    }
  }

  implicit class RTreeFunctionsForTuple[T: ClassTag, S <: Geometry](rdd: RDD[(S, T)]) {
    def buildRTree(numPartitions:Int = -1):RTreeRDD[S, T] = {
      new RTreeRDD[S, T](new RTreeRDDImpl(repartitionRDDorNot(rdd,numPartitions)))
    }

    def buildRTreeWithRepartition(numPartitions: Int, sampleNum:Int = 10000):RTreeRDD[S, T] = {
      require(numPartitions > 0);
      val samplePos = rdd.takeSample(false, sampleNum).map(_._1).array;
      new RTreeRDD[S, T](
        new RTreeRDDImpl(
          new ShuffledRDD(rdd,
            RTreePartitioner.create(samplePos, numPartitions))))
    }
  }

  implicit class RTreeFunctionsForSingle[T: ClassTag, S <: Geometry](rdd: RDD[T]) {
    def buildRTree(f: T => S, numPartitions:Int = -1):RTreeRDD[S, T] = {
      rdd.map(a => (f(a), a)).buildRTree(numPartitions)
    }
    def buildRTreeWithRepartition(f: T => S, numPartitions: Int, sampleNum:Int = 10000):RTreeRDD[S, T] = {
      rdd.map(a => (f(a), a)).buildRTreeWithRepartition(numPartitions, sampleNum)
    }
  }

  implicit class RTreeFunctionsForSparkContext(sc: SparkContext) {
    def rtreeFile[T: ClassTag, U <: Geometry](path:String, partitionPruned:Boolean = false): RTreeRDD[U, T] = {
      val rteeRDD = sc.sequenceFile(path, classOf[NullWritable], classOf[BytesWritable]).map(x => {
        val is = new ByteArrayInputStream(x._2.getBytes);
        val ser = com.github.davidmoten.rtree.Serializers.flatBuffers[T, U]().javaIo[T, U]();
        ser.read(is, x._2.getLength, InternalStructure.SINGLE_ARRAY)
      })
      new RTreeRDD(rteeRDD, partitionPruned)
    }
  }

  implicit class RTreeFunctionsForRTreeRDD[T: ClassTag, U <: Geometry](rdd: RDD[RTree[T, U]]) {

    def getPartitionRecs:Array[Rectangle] = {
      val getPartitionMbr = (tc:TaskContext, iter:Iterator[RTree[T, U]]) => {
        val tree = iter.next();
        val mbrOption = tree.mbr();
        if(iter.hasNext) {
          rdd.logWarning("More than one tree in single partition");
        }
        if(mbrOption.isPresent) {
          Some((tc.partitionId(), mbrOption.get()))
        } else {
          None
        }
      }
      val recArray = new Array[Rectangle](rdd.partitions.length);
      val resultHandler = (index: Int, rst:Option[(Int, Rectangle)]) => {
        rst match {
          case Some((idx, rec)) => require(idx == index)
            recArray(index) = rec
          case None =>
            rdd.logWarning(s"mbr for index ${index} not exist!");
        }
      }
      SparkContext.getOrCreate().runJob[RTree[T, U], Rectangle](this, getPartitionMbr, rdd.partitions.indices, resultHandler);
      recArray
    }
  }



  implicit def en2tup[A, B <: Geometry](a:Entry[A, B]):(B, A) = (a.geometry(), a.value())
  implicit def tup2en[A, B <: Geometry](a:(B, A)):Entry[A, B] = Entries.entry(a._2, a._1)
  implicit def eni2tupi[A, B <: Geometry](iter: Iterator[Entry[A, B]]):Iterator[(B, A)] = iter.map(RTreeRDD.en2tup)
  implicit def tupi2eni[A, B <: Geometry](iter: Iterator[(B, A)]):Iterator[Entry[A, B]] = iter.map(RTreeRDD.tup2en)

  /*implicit def toIterator[A](o:Observable[A]): Iterator[A] = o.toBlocking.getIterator*/  /*new Iterator[A] {
    var rstIter:Option[Iterator[A]] = None

    override def map[B](f: A=>B) = RTreeRDD.toIterator(
      o.map(new Func1[A, B]() {
        override def call(t: A): B = f(t)
      })
    )

    override def flatMap[B](f: A => GenTraversableOnce[B]): Iterator[B] = RTreeRDD.toIterator {
      o.flatMap(new Func1[A, Observable[B]](){
        override def call(t: A): Observable[B] = {
          Observable.from(new Iterable[B] {
            override def iterator: Iterator[B] = f(t).toIterator
          })
        }
      })
    }

    override def filter(f: A => Boolean): Iterator[A] = RTreeRDD.toIterator {
      o.filter(new Func1[A, java.lang.Boolean]() {
        override def call(t: A): java.lang.Boolean = f(t)
      })
    }

    override def foreach[B](f: A=>B):Unit = {
      o.forEach(new Action1[A](){
        override def call(t: A): Unit = f(t)
      })
    }

    override def hasNext = {
      rstIter match {
        case None =>
          rstIter = Some(o.toBlocking.getIterator)
      }
      rstIter.get.hasNext
    }

    override def next = {
      rstIter match {
        case None =>
          rstIter = Some(o.toBlocking.getIterator)
      }
      rstIter.get.next
    }
  }*/
}




private[spark] class RTreeRDD[U <: Geometry, T: ClassTag] (var prev: RDD[RTree[T, U]], var partitionPruned:Boolean = false)
  extends RDD[(U, T)](prev) {

  //prev.cache()


  private var _partitionRecs:Array[Rectangle] = null;
  val partitionRecs:Array[Rectangle] = {
    if(_partitionRecs == null) {
      _partitionRecs = prev.getPartitionRecs
      require(_partitionRecs.length == partitions.length)
      _partitionRecs.foreach(println)
    }
    _partitionRecs
  }


  /*
  private var rtree: Option[RTree[T, U]] = None

  private def getPartitionRTree(split: Partition, context: TaskContext): RTree[T, U] = {
    rtree match {
      case None =>
        val tree: RTree[T, U] = RTree.star().create()
        firstParent[T].iterator(split, context).foreach(a => tree.add(Entry.entry(a, f(a))))
        rtree = Some(tree)
        val rst = tree.entries()
        rst.toBlocking.first()
        rtree.get
      case Some(tree) =>
        tree
    }
  }
  */

  def search(r:Rectangle):RDD[(U, T)] = {
    (if(partitionPruned) {
      PartitionPruningRDD.create(prev, partitionRecs(_).intersects(r))
    } else {
      firstParent[RTree[T, U]]
    }).mapPartitions(iter => {
      val it = RTreeRDD.eni2tupi(iter.next().search(r).toBlocking.getIterator)
      it
    })
  }

  def saveAsRTreeFile(path:String):Unit = {
    prev
      .map(tree => {
        val os = new ByteArrayOutputStream();
        val ser = com.github.davidmoten.rtree.Serializers.flatBuffers[T, U]().javaIo[T, U]();
        ser.write(tree, os);
        (NullWritable.get(), new BytesWritable(os.toByteArray))
      })
      .saveAsSequenceFile(path)
  }

  override def getPartitions: Array[Partition] = firstParent[RTree[T, U]].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[(U, T)] = {
    val it = RTreeRDD.eni2tupi(firstParent[RTree[T, U]].iterator(split, context).next().entries().toBlocking.getIterator)
    it
  }

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }
}

