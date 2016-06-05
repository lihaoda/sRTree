package edu.thu.cs.database.spark.rdd

//import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

//import scala.collection.JavaConverters._
import java.io._

import edu.thu.cs.database.spark.RTreeInputFormat
import org.apache.hadoop.io.{BytesWritable, NullWritable}

import scala.collection.{TraversableOnce, mutable}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

//import com.github.davidmoten.rtree.geometry.{Geometry, Rectangle, Geometries}
//import com.github.davidmoten.rtree.{InternalStructure, Entry, RTree, Entries}
//import org.apache.hadoop.io.{BytesWritable, NullWritable}
//import org.apache.hadoop.mapred.SequenceFileInputFormat
//import edu.thu.cs.database.spark.RTreeInputFormat
import edu.thu.cs.database.spark.rtree._
import edu.thu.cs.database.spark.spatial._
import edu.thu.cs.database.spark.partitioner.RTreePartitioner
import org.apache.spark.rdd.{ShuffledRDD, PartitionPruningRDD, RDD}
import org.apache.spark._
import org.apache.spark.util.random.BernoulliSampler
//import scala.collection.JavaConversions.asScalaIterator
import scala.reflect.ClassTag

/**
  * Created by lihaoda on 16-3-23.
  */

class SearchedIterator[T: ClassTag](data:Array[T], rstIter: Iterator[(Shape, Int)]) extends Iterator[(Point, T)] {
  override def hasNext: Boolean = rstIter.hasNext
  override def next(): (Point, T) = {
    val rst = rstIter.next()
    (rst._1.asInstanceOf[Point], data(rst._2))
  }
}


class PointDisOrdering[T: ClassTag](origin: Point) extends Ordering[(Point, T)] {
  def compare(a: (Point, T), b: (Point, T)) = {
    origin.minDist(a._1).compare(origin.minDist(b._1))
  }
}

object RTreeRDD {

  def test(): Unit = {

    val data = SparkContext.getOrCreate().textFile("/home/spark/test_data/small.txt").map ( s => {
      val strs = s.split(" ")
      (Point(Array(strs(1).toDouble, strs(2).toDouble)), strs(0).toInt)
    }).reduceByKey((a,b) => {
      a
    },100)
    //val rtreeRDD = data.buildRTree()

    val xrec = MBR(Point(Array(40,115)),Point(Array(41,116)))

    val oneRst = data.filter(a => {
      xrec.intersects(a._1)
    })
    val st = System.currentTimeMillis
    val cnt = oneRst.count()
    println(s"multiResult count: ${cnt}")
    val ed = System.currentTimeMillis
  }

  class RTreeRDDImpl[T: ClassTag](rdd: RDD[(Point, T)], max_entry_per_node:Int = RTree.default_max_entry_per_node) extends RDD[(RTree, Array[T])](rdd) {
    override def getPartitions: Array[Partition] = firstParent[(Point, T)].partitions
    override def compute(split: Partition, context: TaskContext): Iterator[(RTree, Array[T])] = {
      val b = firstParent[(Point, T)].iterator(split, context).toArray
      if(b.nonEmpty) {
        val tree = RTree(b.map(_._1).zipWithIndex, max_entry_per_node)
        Iterator((tree, b.map(_._2)))
      } else {
        Iterator()
      }
    }
  }

  def getActualSavePath(path:String) = {
    if(path.endsWith("/") || path.endsWith("\\")) {
      (path+"data", path+"global")
    } else {
      (path+"/data", path+"/global")
    }
  }

  def repartitionRDDorNot[T: ClassTag](rdd: RDD[T], numPartitions: Int): RDD[T] = {
    if (numPartitions > 0 && numPartitions != rdd.partitions.length) {
      rdd.repartition(numPartitions)
    } else {
      rdd
    }
  }

  def getSamplesWithBound[T: ClassTag](rdd: RDD[(Point, T)], sampleRate: Double): (Array[Point], MBR) = {

    def updatePointCoord(origin: Point, p: Point, bigger:Boolean): Unit = {
      for(i <- p.coord.indices) {
        if(origin.coord(i) > p.coord(i) && !bigger) {
          origin.coord(i) = p.coord(i)
        } else if (origin.coord(i) < p.coord(i) && bigger) {
          origin.coord(i) = p.coord(i)
        }
      }
    }

    val getPartitionMBRAndSamples = (tc:TaskContext, iter:Iterator[Point]) => {
      val sampler = new BernoulliSampler[Point](sampleRate)
      sampler.setSeed(new java.util.Random().nextLong())
      val recordIter = new Iterator[Point]() {
        var lowBound: Option[Point] = None
        var highBound: Option[Point] = None
        def updateBound(p:Point): Unit = {
          lowBound match {
            case Some(low) =>
              updatePointCoord(low, p, false)
            case None =>
              lowBound = Some(Point(p.coord.clone()))
          }
          highBound match {
            case Some(high) =>
              updatePointCoord(high, p, true)
            case None =>
              highBound = Some(Point(p.coord.clone()))
          }
        }
        override def hasNext: Boolean = iter.hasNext
        override def next(): Point = {
          val p = iter.next()
          updateBound(p)
          p
        }
      }
      val samples = sampler.sample(recordIter).toArray
      if(recordIter.lowBound.isDefined && recordIter.highBound.isDefined)
        Some((new MBR(recordIter.lowBound.get, recordIter.highBound.get), samples))
      else
        None
    }
    var bound: Option[Point] = None
    val points = new mutable.ArrayBuffer[Point]()
    val recs = new ListBuffer[MBR]()
    val resultHandler:(Int, Option[(MBR, Array[Point])]) => Unit = (index, rst) => {
      rst match {
        case Some((mbr, ps)) =>
          recs += mbr
          points ++= ps
        case None =>
          println(s"Error! MBR for part ${index} not exist!")
      }
      //return
    }
    val pointRDD = rdd.map(_._1)
    SparkContext.getOrCreate().runJob(pointRDD, getPartitionMBRAndSamples, pointRDD.partitions.indices, resultHandler)
    //recs.foreach(println)
    (points.toArray, recs.reduce((a, b) => {
      //println("a before: " + a)
      //println("b before: " + b)
      updatePointCoord(a.low, b.low, false)
      updatePointCoord(a.high, b.high, true)
      //println("a after: " + a)
      a
    }))
  }

  implicit class RTreeFunctionsForTuple[T: ClassTag](rdd: RDD[(Point, T)]) {
    def buildRTree(numPartitions:Int = -1):RTreeRDD[T] = {
      new RTreeRDD[T](new RTreeRDDImpl(repartitionRDDorNot(rdd,numPartitions)))
    }

    def buildRTreeWithRepartition(numPartitions: Int, sampleRate:Double = 0.0001):RTreeRDD[T] = {
      //require(numPartitions > 0)
      //rdd.cache()
      /*
      val samplePos = rdd.map(_._1).sample(false, sampleRate).collect()
      */
      val (samplePos, bound) = getSamplesWithBound(rdd, sampleRate)
      val rddPartitioner = RTreePartitioner.create(samplePos, numPartitions, bound)
      val shuffledRDD = new ShuffledRDD[Point, T, T](rdd, rddPartitioner)
      val rtreeImpl = new RTreeRDDImpl(shuffledRDD)
      new RTreeRDD[T](rtreeImpl)
    }
  }

  implicit class RTreeFunctionsForSingle[T: ClassTag](rdd: RDD[T]) {
    def buildRTree(f: T => Point, numPartitions:Int = -1):RTreeRDD[T] = {
      rdd.map(a => (f(a), a)).buildRTree(numPartitions)
    }
    def buildRTreeWithRepartition(f: T => Point, numPartitions: Int, sampleRate:Double = 0.0001):RTreeRDD[T] = {
      rdd.map(a => (f(a), a)).buildRTreeWithRepartition(numPartitions, sampleRate)
    }
  }

  implicit class RTreeFunctionsForSparkContext(sc: SparkContext) {
    def rtreeFile[T : ClassTag](path:String): RTreeRDD[T] = {
      val paths = getActualSavePath(path)
      val inputFormatClass = classOf[RTreeInputFormat[NullWritable, BytesWritable]]
      val seqRDD = sc.hadoopFile(paths._1, inputFormatClass, classOf[NullWritable], classOf[BytesWritable])
      val rtreeRDD = seqRDD.map(x => {
        val bis = new ByteArrayInputStream(x._2.getBytes)
        val ois = new ObjectInputStream(bis)
        ois.readObject.asInstanceOf[(RTree, Array[T])]
      })
      val rdd = new RTreeRDD[T](rtreeRDD)  //rtreeDataFile[T](paths._1, partitionPruned)
      val global = sc.objectFile[RTree](paths._2).collect().head
      rdd.setGlobalRTree(global)
      rdd
    }
  }

  implicit class RTreeFunctionsForRTreeRDD[T: ClassTag](rdd: RDD[(RTree, Array[T])]) {

    def getPartitionRecs:Array[(MBR, Int, Long)] = {
      val getPartitionMbr = (tc:TaskContext, iter:Iterator[(RTree, Array[T])]) => {
        if(iter.hasNext) {
          val tree = iter.next()._1
          val mbr = tree.root.m_mbr
          Some((mbr, tc.partitionId(), tree.root.size))
        } else {
          None
        }
      }
      val recArray = new Array[(MBR, Int, Long)](rdd.partitions.length)
      val resultHandler = (index: Int, rst:Option[(MBR, Int, Long)]) => {
        rst match {
          case Some((rec, idx, len)) =>
            require(idx == index)
            recArray(index) = (rec, idx, len)
          case None =>
        }
      }
      SparkContext.getOrCreate().runJob(rdd, getPartitionMbr, rdd.partitions.indices, resultHandler)
      recArray
    }
  }
}




private[spark] class RTreeRDD[T: ClassTag] (var prev: RDD[(RTree, Array[T])])
  extends RDD[(Point, T)](prev) {

  //prev.cache()

  @transient
  //private var _partitionRecs:Array[(MBR, Int, Long)] = null
  private var _globalRTree: RTree = null

  def setGlobalRTree(tree:RTree) = {
    //recs.zipWithIndex.foreach(println)
    _globalRTree = tree
    this
  }

  def impl = prev

  def globalRTree:RTree = {
    import RTreeRDD._
    if(_globalRTree == null) {
      //prev.cache()
      val recs = prev.getPartitionRecs
      require(recs.length == partitions.length)
      _globalRTree = RTree(recs, RTree.default_max_entry_per_node)
      //_partitionRecs.zipWithIndex.foreach(println)
    }
    _globalRTree
  }

  override def cache() = {
    prev.cache()
    this
  }

  private def joinRDDWithPartition[W: ClassTag, U: ClassTag](rdd:RTreeRDD[W],
                                                joinedParts:TraversableOnce[(Int, Int)],
                                                func:((Point, T), (RTree, Array[W])) => TraversableOnce[U])
  :RDD[((Point, T),TraversableOnce[U])] = {
    val leftTmpMap = new mutable.HashMap[Int, mutable.ArrayBuffer[(Int, Int)]]()
    val rightTmpMap = new mutable.HashMap[Int, mutable.ArrayBuffer[(Int, Int)]]()

    joinedParts.foreach(t => {
      if(!leftTmpMap.contains(t._1)) {
        leftTmpMap += Tuple2(t._1, new  mutable.ArrayBuffer[(Int, Int)]())
      }
      if(!rightTmpMap.contains(t._2)) {
        rightTmpMap += Tuple2(t._2, new  mutable.ArrayBuffer[(Int, Int)]())
      }
      leftTmpMap(t._1) += t
      rightTmpMap(t._2) += t
    })

    //println("left map size:" + leftTmpMap.size)
    //println("right map size:" + rightTmpMap.size)

    val leftImpl = this.impl
    val rightImpl = rdd.impl
    val left = getKeySetedRDD(leftImpl, leftTmpMap.map(a => (a._1, a._2.toList)))
    val right = getKeySetedRDD(rightImpl, rightTmpMap.map(a => (a._1, a._2.toList)))

    //println("left size:" + left.count())
    //println("right size:" + right.count())

    val cogrouped = left.cogroup(right)
    //println("co size:" + cogrouped.count())
    cogrouped.flatMap(t => {
      val partIds = t._1
      val aData = t._2._1
      val bData = t._2._2
      if(aData.nonEmpty && bData.nonEmpty) {
        val rst = new ArrayBuffer[((Point, T),TraversableOnce[U])]()
        aData.foreach( a => {
          val apois = a._1.all
          val adata = a._2
          bData.foreach( b => {
            apois.foreach(t => {
              val ap = t._1.asInstanceOf[Point]
              val aindex = t._2
              rst += Tuple2((ap, adata(aindex)), func((ap, adata(aindex)), b))
            })
          })
        })
        rst
      } else {
        Iterator()
      }
    })
  }

  private def getKeySetedRDD[U:ClassTag]
  (rdd: RDD[(RTree, Array[U])], m: mutable.Map[Int, collection.immutable.List[(Int, Int)]]) = {
    rdd.mapPartitionsWithIndex((idx, iter) => {
      iter.flatMap(p => {
        if(m.contains(idx)) {
          m(idx).map(a => (a, p))
        } else {
          List()
        }
      })
    })
  }

  def saveAsRTreeFile(path:String):Unit = {
    val paths = RTreeRDD.getActualSavePath(path)
    //prev.cache()
    prev
      .map(x => {
        val bos = new ByteArrayOutputStream()
        val oos = new ObjectOutputStream(bos)
        oos.writeObject(x)
        oos.close()
        (NullWritable.get(), new BytesWritable(bos.toByteArray))
      })
      .saveAsSequenceFile(paths._1)
    sparkContext.parallelize(Array(globalRTree)).saveAsObjectFile(paths._2)
  }

  def search(r:MBR):RDD[(Point, T)] = {
    require(r.low.coord.length == globalRTree.dim)

    val rst = globalRTree.range(r)
    val tmpSet = new mutable.HashSet[Int]()
    tmpSet ++= rst.map(_._2)
    PartitionPruningRDD.create(prev, tmpSet.contains).mapPartitions(iter => {
      if (iter.hasNext) {
        val (tree, data) = iter.next()
        new SearchedIterator[T](data, tree.range(r).iterator)
      } else {
        Iterator()
      }
    })
  }

  def search(p:Point, r:Double): RDD[(Point, T)] = {
    require(p.coord.length == globalRTree.dim)
    val rst = globalRTree.circleRange(p, r)
    val tmpSet = new mutable.HashSet[Int]()
    tmpSet ++= rst.map(_._2)
    PartitionPruningRDD.create(prev, tmpSet.contains).mapPartitions(iter => {
      if (iter.hasNext) {
        val (tree, data) = iter.next()
        new SearchedIterator[T](data, tree.circleRange(p, r).iterator)
      } else {
        Iterator()
      }
    })
  }

  def distJoin[W: ClassTag](rdd:RTreeRDD[W], dist:Double):RDD[((Point, T),(Point, W))] = {

    //println("========= start dist join =========")
    val joinParts = new mutable.ArrayBuffer[(Int, Int)]()
    globalRTree.all.foreach(a => {
      val mbr = a._1.asInstanceOf[MBR]
      rdd.globalRTree.circleRange(mbr, dist).foreach(b => {
        joinParts += Tuple2(a._2, b._2)
      })
    })
    joinParts.foreach(println)
    //println("===========================")

    val rst = joinRDDWithPartition(rdd, joinParts, (a:(Point, T), b:(RTree, Array[W])) => {
      val point = a._1
      val adata = a._2
      val tree = b._1
      val bdatas = b._2
      val rst = tree.circleRange(point, dist).map(t => {
        (t._1.asInstanceOf[Point], bdatas(t._2))
      })
      rst
    })

    //println("========= end dist join =========")
    rst.flatMap(t => {
      val at = t._1
      t._2.map[((Point, T), (Point, W))](b => (at, b))
    })
  }


  def maxDist = (a: Point, b: MBR) => {
    var ans = 0.0
    for (i <- a.coord.indices) {
      ans += Math.max((a.coord(i) - b.low.coord(i)) * (a.coord(i) - b.low.coord(i)),
        (a.coord(i) - b.high.coord(i)) * (a.coord(i) - b.high.coord(i)))
    }
    Math.sqrt(ans)
  }

  def knnJoin[W: ClassTag](rdd:RTreeRDD[W], k:Int):RDD[((Point, T),(Point, W))] = {

    def centerPoint(mbr:MBR):Point = {
      val rst = new Array[Double](mbr.low.coord.length)
      for(i <- mbr.low.coord.indices) {
        rst(i) = (mbr.low.coord(i) + mbr.high.coord(i))/2
      }
      Point(rst)
    }

    def getLooseDistByKNN(knnDist:Double, mbr:MBR, center:Point): Double = {
      maxDist(center, mbr) * 2 + knnDist
    }

    val joinParts = new mutable.ArrayBuffer[(Int, Int)]()
    globalRTree.all.foreach(a => {
      val center = centerPoint(a._1.asInstanceOf[MBR])
      val knnDis = rdd.takeKNN(center, k).last._1.minDist(center)
      val dis = getLooseDistByKNN(knnDis, a._1.asInstanceOf[MBR], center)
      rdd.globalRTree.circleRange(center, dis).foreach(b => {
        joinParts += Tuple2(a._2, b._2)
      })
    })

    joinRDDWithPartition(rdd, joinParts, (a:(Point, T), b:(RTree, Array[W])) => {
      val point = a._1
      val adata = a._2
      val tree = b._1
      val bdatas = b._2
      tree.kNN(point, k).map(t => {
        (point.minDist(t._1), (t._1.asInstanceOf[Point], bdatas(t._2)))
      })
    }).reduceByKey(_.toList ++ _)
      .flatMap[((Point, T), (Point, W))](l => {
        l._2
          .toList
          .sortWith((a,b) => {
            a._1 < b._1
          }).take(k).map(t => {
          (l._1, t._2)
        })
      })
  }

  def takeKNN(p:Point, k:Int): Array[(Point, T)] = {
    require(p.coord.length == globalRTree.dim)
    val ord = new PointDisOrdering[T](p)

    val recs = globalRTree.kNN(p, maxDist, k, false)
    val tmpSet = new mutable.HashSet[Int]()
    tmpSet ++= recs.map(_._2)

    val tmpRst = PartitionPruningRDD.create(prev, tmpSet.contains).mapPartitions(iter => {
      if (iter.hasNext) {
        val (tree, data) = iter.next()
        new SearchedIterator[T](data, tree.kNN(p, k).iterator)
      } else {
        Iterator()
      }
    }).takeOrdered(k)(ord)
    val range = p.minDist(tmpRst.last._1)
    val otherRecs = globalRTree.circleRange(p, range).iterator.map(_._2).filter(!tmpSet.contains(_))
    if(otherRecs.nonEmpty) {
      val tmpSet2 = new mutable.HashSet[Int]()
      tmpSet2 ++= otherRecs
      val rst = PartitionPruningRDD.create(prev, tmpSet2.contains).mapPartitions(iter => {
        if (iter.hasNext) {
          val (tree, data) = iter.next()
          new SearchedIterator[T](data, tree.kNN(p, k).iterator)
        } else {
          Iterator()
        }
      }).takeOrdered(k)(ord)
      (rst ++ tmpRst).sortWith((a,b) => {
        p.minDist(a._1) < p.minDist(b._1)
      }).take(k)
    } else {
      tmpRst
    }
  }

  override def getPartitions: Array[Partition] = firstParent[(RTree, Array[T])].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[(Point, T)] = {
    val iter = firstParent[(RTree, Array[T])].iterator(split, context)
    if (iter.hasNext) {
      val (tree, data) = iter.next()
      new SearchedIterator[T](data, tree.range(tree.root.m_mbr).iterator)
    } else {
      Iterator()
    }
  }

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }
}

