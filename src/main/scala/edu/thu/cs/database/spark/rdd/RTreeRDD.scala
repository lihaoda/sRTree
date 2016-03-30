package edu.thu.cs.database.spark.rdd

import com.github.davidmoten.rtree.geometry.{Geometry, Rectangle}
import com.github.davidmoten.rtree.{Entry, RTree, Entries}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

import scala.collection.JavaConversions.asScalaIterator
import scala.reflect.ClassTag

/**
  * Created by lihaoda on 16-3-23.
  */

object RTreeRDD {

  implicit class RTreeFunctionsForTuple[T: ClassTag, S <: Geometry](rdd: RDD[(S, T)]) {
    def buildRTree(numPartitions:Int = -1):RTreeRDD[S, T] = {
      val partitionedRdd = {
        if(numPartitions > 0) {
          rdd.repartition(numPartitions)
        } else {
          rdd
        }
      }
      new RTreeRDD[S, T](
        partitionedRdd.mapPartitions(iter => {
          println("build tree ....")
          println("iter length:" + iter.length)
          var tree = RTree.star().createRTree[T, S]()
          var k = 0
          while(iter.hasNext) {
            k += 1
            println("in: " + k)
            tree = tree.add(iter.next())
          }
          println("out: " + k)
          Iterator(tree)
        }, true)
      )
    }
  }

  implicit class RTreeFunctionsForSingle[T: ClassTag, S <: Geometry](rdd: RDD[T]) {
    def buildRTree(f: T => S, numPartitions:Int = -1):RTreeRDD[S, T] = {
      val partitionedRdd = {
        if(numPartitions > 0) {
          rdd.repartition(numPartitions)
        } else {
          rdd
        }
      }
      new RTreeRDD[S, T](
        partitionedRdd.mapPartitions(iter => {
          var tree: RTree[T, S] = RTree.star().create()
          while(iter.hasNext) {
            val a = iter.next()
            tree = tree.add((f(a), a))
          }
          Iterator(tree)
        },
          true)
      )
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




private[spark] class RTreeRDD[U <: Geometry, T: ClassTag] (var prev: RDD[RTree[T, U]])
  extends RDD[(U, T)](prev) {

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
    firstParent[RTree[T, U]].mapPartitions(iter => {
      val it = RTreeRDD.eni2tupi(iter.next().search(r).toBlocking.getIterator)
      it
    })
  }

  def saveAsIndexFile(path:String):Unit = {

  }



  override val partitioner = firstParent[RTree[T, U]].partitioner

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

