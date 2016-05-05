package edu.thu.cs.database.spark.rdd

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.github.davidmoten.rtree.geometry.{Geometry, Rectangle, Geometries}
import com.github.davidmoten.rtree.{InternalStructure, Entry, RTree, Entries}
import edu.thu.cs.database.spark.partitioner.RTreePartitioner
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.rdd.{ShuffledRDD, PartitionPruningRDD, RDD}
import org.apache.spark._
import rx.functions.Func1

import scala.collection.JavaConversions.asScalaIterator
import scala.reflect.ClassTag

/**
  * Created by lihaoda on 16-3-23.
  */

object RTreeRDD {

  class RTreeRDDImpl[U <: Geometry: ClassTag, T: ClassTag](rdd: RDD[(U, T)]) extends RDD[RTree[T, U]](rdd) {
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
    if (numPartitions > 0 && numPartitions != rdd.getNumPartitions) {
      rdd.repartition(numPartitions)
    } else {
      rdd
    }
  }

  implicit class RTreeFunctionsForTuple[T: ClassTag, S <: Geometry : ClassTag](rdd: RDD[(S, T)]) {
    def buildRTree(numPartitions:Int = -1):RTreeRDD[S, T] = {
      new RTreeRDD[S, T](new RTreeRDDImpl(repartitionRDDorNot(rdd,numPartitions)))
    }

    def buildRTreeWithRepartition(numPartitions: Int, sampleNum:Int = 10000):RTreeRDD[S, T] = {
      require(numPartitions > 0);
      rdd.cache();
      val samplePos = rdd.takeSample(false, sampleNum).map(_._1);
      val rddPartitioner = RTreePartitioner.create(samplePos, numPartitions);
      val shuffledRDD = new ShuffledRDD[S, T, T](rdd, rddPartitioner);
      val rtreeImpl = new RTreeRDDImpl(shuffledRDD);
      new RTreeRDD[S, T](rtreeImpl)
    }
  }

  implicit class RTreeFunctionsForSingle[T: ClassTag, S <: Geometry : ClassTag](rdd: RDD[T]) {
    def buildRTree(f: T => S, numPartitions:Int = -1):RTreeRDD[S, T] = {
      rdd.map(a => (f(a), a)).buildRTree(numPartitions)
    }
    def buildRTreeWithRepartition(f: T => S, numPartitions: Int, sampleNum:Int = 10000):RTreeRDD[S, T] = {
      rdd.map(a => (f(a), a)).buildRTreeWithRepartition(numPartitions, sampleNum)
    }
  }

  implicit def toFunc1[A, B](a: A => B):Func1[A, B] = new Func1[A, B] {
    override def call(t: A): B = a(t)
  }

  implicit class RTreeFunctionsForSparkContext(sc: SparkContext) {
    def rtreeFile[T : ClassTag, U <: Geometry : ClassTag](path:String,
                                                          ser: T => Array[Byte],
                                                          deser: Array[Byte] => T,
                                                          partitionPruned:Boolean = true): RTreeRDD[U, T] = {

      val rtreeRDD = sc.sequenceFile(path, classOf[NullWritable], classOf[BytesWritable]).map(x => {
        val is = new ByteArrayInputStream(x._2.getBytes);
        val xser = com.github.davidmoten.rtree.Serializers.flatBuffers[T, U]()
          .serializer[T](RTreeRDD.toFunc1(ser))
          .deserializer(RTreeRDD.toFunc1(deser))
          .create[U]()
        xser.read(is, x._2.getLength, InternalStructure.SINGLE_ARRAY)
      })

      //val rtreeRDD = sc.objectFile[RTree[T, U]](path);
      new RTreeRDD(rtreeRDD, partitionPruned)
    }
  }

  implicit class RTreeFunctionsForRTreeRDD[T: ClassTag, U <: Geometry : ClassTag](rdd: RDD[RTree[T, U]]) {

    def getPartitionRecs:Array[Rectangle] = {
      val getPartitionMbr = (tc:TaskContext, iter:Iterator[RTree[T, U]]) => {
        if(iter.hasNext) {
          val tree = iter.next();
          val mbrOption = tree.mbr();/*
        if(iter.hasNext) {
          ;//rdd.logWarning("More than one tree in single partition");
        }*/
          if(mbrOption.isPresent) {
            Some((tc.partitionId(), mbrOption.get()))
          } else {
            /*
            if(tree.isReloaded())
              Some(tc.partitionId(), Geometries.rectangle(0,0,0,0))
            else
              Some(tc.partitionId(), Geometries.rectangle(0,0,1,1))
              */
            None
          }
        } else {
          None
        }
        //Some(tc.partitionId(), Geometries.rectangle(0,0,0,0))
      }
      val recArray = new Array[Rectangle](rdd.partitions.length);
      val resultHandler = (index: Int, rst:Option[(Int, Rectangle)]) => {
        rst match {
          case Some((idx, rec)) =>
            require(idx == index)
            recArray(index) = rec
          case None =>
            //rdd.logWarning(s"mbr for index ${index} not exist!");
        }
      }
      SparkContext.getOrCreate().runJob(rdd, getPartitionMbr, rdd.partitions.indices, resultHandler);
      recArray.zipWithIndex.foreach(println)
      recArray
    }
  }






  implicit def en2tup[A, B <: Geometry : ClassTag](a:Entry[A, B]):(B, A) = (a.geometry(), a.value())
  implicit def tup2en[A, B <: Geometry : ClassTag](a:(B, A)):Entry[A, B] = Entries.entry(a._2, a._1)
  implicit def eni2tupi[A, B <: Geometry : ClassTag](iter: Iterator[Entry[A, B]]):Iterator[(B, A)] = iter.map(en2tup[A,B])
  implicit def tupi2eni[A, B <: Geometry : ClassTag](iter: Iterator[(B, A)]):Iterator[Entry[A, B]] = iter.map(tup2en[A,B])

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




private[spark] class RTreeRDD[U <: Geometry : ClassTag, T: ClassTag] (var prev: RDD[RTree[T, U]], @transient var partitionPruned:Boolean = true)
  extends RDD[(U, T)](prev) {

  prev.cache()

  @transient
  private var _partitionRecs:Array[Rectangle] = null;

  def partitionRecs:Array[Rectangle] = {
    import RTreeRDD._
    if(_partitionRecs == null && partitionPruned) {
      _partitionRecs = prev.getPartitionRecs
      require(_partitionRecs.length == partitions.length)
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

  def saveAsRTreeFile(path:String, ser: T => Array[Byte], deser: Array[Byte] => T):Unit = {
    prev
      .map(tree => {
        var os:ByteArrayOutputStream = null;
        val getSerilized = () => {
          os = new ByteArrayOutputStream();
          val xser = com.github.davidmoten.rtree.Serializers.flatBuffers[T, U]()
            .serializer[T](RTreeRDD.toFunc1(ser))
            .deserializer(RTreeRDD.toFunc1(deser))
            .create[U]()
          xser.write(tree, os);
        }
        try {
          getSerilized();
        } catch  {
          case _: Throwable =>
            os = null;
            System.gc();
            getSerilized();
        }
        (NullWritable.get(), new BytesWritable(os.toByteArray))
      })
      .saveAsSequenceFile(path)

  }

  def search(r:Rectangle):RDD[(U, T)] = {
    (if(partitionPruned) {
      PartitionPruningRDD.create(prev, idx => {
        if(partitionRecs(idx) == null)
          false
        else
          partitionRecs(idx).intersects(r)
      })
    } else {
      firstParent[RTree[T, U]]
    }).mapPartitions(iter => {
      if (iter.hasNext) {
        RTreeRDD.eni2tupi(iter.next().search(r).toBlocking.getIterator)
      } else {
        Iterator()
      }
    })
  }

  override def getPartitions: Array[Partition] = firstParent[RTree[T, U]].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[(U, T)] = {
    val iter = firstParent[RTree[T, U]].iterator(split, context)
    if (iter.hasNext) {
      RTreeRDD.eni2tupi(iter.next().entries().toBlocking.getIterator)
    } else {
      Iterator()
    }
  }

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }
}

