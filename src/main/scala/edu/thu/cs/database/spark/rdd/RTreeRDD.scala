package edu.thu.cs.database.spark.rdd

//import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

//import scala.collection.JavaConverters._
import java.io._

import edu.thu.cs.database.spark.RTreeInputFormat
import org.apache.hadoop.io.{BytesWritable, NullWritable}

import scala.collection.mutable

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

  class RTreeRDDImpl[T: ClassTag](rdd: RDD[(Point, T)], max_entry_per_node:Int = 25) extends RDD[(RTree, Array[(Point, T)])](rdd) {
    override def getPartitions: Array[Partition] = firstParent[(Point, T)].partitions
    override def compute(split: Partition, context: TaskContext): Iterator[(RTree, Array[(Point, T)])] = {
      val b = firstParent[(Point, T)].iterator(split, context).toArray
      if(b.nonEmpty) {
        val tree = RTree(b.map(_._1).zipWithIndex, max_entry_per_node)
        Iterator((tree, b))
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
      var lowBound: Option[Point] = None
      var highBound: Option[Point] = None

      val samples = sampler.sample(new Iterator[Point]() {
        def updateBound(p:Point): Unit = {
          lowBound match {
            case Some(low) =>
              updatePointCoord(low, p, false)
            case None =>
              lowBound = Some(p.copy())
          }
          highBound match {
            case Some(high) =>
              updatePointCoord(high, p, false)
            case None =>
              highBound = Some(p.copy())
          }
        }
        override def hasNext: Boolean = iter.hasNext
        override def next(): Point = {
          val p = iter.next()
          updateBound(p)
          p
        }
      }).toArray
      if(lowBound != None && highBound != None)
        Some((new MBR(lowBound.get, highBound.get), samples))
      else
        None
    }
    var recMBR: Option[MBR] = None
    val recArray = new mutable.ArrayBuffer[Point]()
    val resultHandler:(Int, Option[(MBR, Array[Point])]) => Unit = (index, rst) => {
      rst match {
        case Some((mbr, points)) =>
          println(mbr)
          recMBR match {
            case None => recMBR = Some(mbr)
            case Some(m) =>
              updatePointCoord(m.low, mbr.low, false)
              updatePointCoord(m.high, mbr.high, true)
          }
          recArray ++= points
        case None =>
          println(s"Error! MBR for part ${index} not exist!")
      }
      //return
    }
    val pointRDD = rdd.map(_._1)
    SparkContext.getOrCreate().runJob(pointRDD, getPartitionMBRAndSamples, pointRDD.partitions.indices, resultHandler)
    require(recMBR.isDefined)
    println("===============")
    (recArray.toArray, recMBR.get)
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
      new RTreeRDD[T](rtreeImpl).setPartitionRecs(rddPartitioner.mbrs)
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
    def rtreeFile[T : ClassTag](path:String, partitionPruned:Boolean = true): RTreeRDD[T] = {
      val paths = getActualSavePath(path)
      val inputFormatClass = classOf[RTreeInputFormat[NullWritable, BytesWritable]]
      val seqRDD = sc.hadoopFile(paths._1, inputFormatClass, classOf[NullWritable], classOf[BytesWritable])
      val rtreeRDD = seqRDD.map(x => {
        val bis = new ByteArrayInputStream(x._2.getBytes)
        val ois = new ObjectInputStream(bis)
        ois.readObject.asInstanceOf[(RTree, Array[(Point, T)])]
      })
      val rdd = new RTreeRDD[T](rtreeRDD, partitionPruned)  //rtreeDataFile[T](paths._1, partitionPruned)
      val global = sc.objectFile[(MBR, Int)](paths._2).collect().sortBy(_._2).map(_._1)
      rdd.setPartitionRecs(global)
      rdd
    }
  }

  implicit class RTreeFunctionsForRTreeRDD[T: ClassTag](rdd: RDD[(RTree, Array[(Point, T)])]) {

    def getPartitionRecs:Array[MBR] = {
      val getPartitionMbr = (tc:TaskContext, iter:Iterator[(RTree, Array[(Point, T)])]) => {
        if(iter.hasNext) {
          val tree = iter.next()._1
          val mbr = tree.root.m_mbr
          Some((tc.partitionId(), mbr))
        } else {
          None
        }
      }
      val recArray = new Array[MBR](rdd.partitions.length)
      val resultHandler = (index: Int, rst:Option[(Int, MBR)]) => {
        rst match {
          case Some((idx, rec)) =>
            require(idx == index)
            recArray(index) = rec
          case None =>
        }
      }
      SparkContext.getOrCreate().runJob(rdd, getPartitionMbr, rdd.partitions.indices, resultHandler)
      recArray
    }
  }
}




private[spark] class RTreeRDD[T: ClassTag] (var prev: RDD[(RTree, Array[(Point, T)])], @transient var partitionPruned:Boolean = true)
  extends RDD[(Point, T)](prev) {

  //prev.cache()


  @transient
  private var _partitionRecs:Array[MBR] = null

  def setPartitionRecs(recs:Array[MBR]) = {
    //recs.zipWithIndex.foreach(println)
    require(recs.length == partitions.length)
    _partitionRecs = recs
    this
  }

  def impl = prev

  def partitionRecs:Array[MBR] = {
    import RTreeRDD._
    if(_partitionRecs == null && partitionPruned) {
      //prev.cache()
      _partitionRecs = prev.getPartitionRecs
      require(_partitionRecs.length == partitions.length)
      //_partitionRecs.zipWithIndex.foreach(println)
    }
    _partitionRecs
  }

  override def cache() = {
    prev.cache()
    this
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
    sparkContext.parallelize(partitionRecs.zipWithIndex).saveAsObjectFile(paths._2)
  }

  def search(r:MBR):RDD[(Point, T)] = {
    (if(partitionPruned) {
      //prev.cache()
      PartitionPruningRDD.create(prev, idx => {
        if(partitionRecs(idx) == null) {
          false
        } else {
          val rst = partitionRecs(idx).intersects(r)
          //println(idx, rst)
          rst
        }
      })
    } else {
      firstParent[(RTree, Array[(Point, T)])]
    }).mapPartitions(iter => {
      if (iter.hasNext) {
        new Iterator[(Point, T)](){
          val data = iter.next()
          val rstIter = data._1.range(r).iterator
          override def hasNext: Boolean = rstIter.hasNext
          override def next(): (Point, T) = data._2(rstIter.next()._2)
        }
      } else {
        Iterator()
      }
    })
  }

  override def getPartitions: Array[Partition] = firstParent[(RTree, Array[(Point, T)])].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[(Point, T)] = {
    val iter = firstParent[(RTree, Array[(Point, T)])].iterator(split, context)
    if (iter.hasNext) {
      iter.next()._2.iterator
    } else {
      Iterator()
    }
  }

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }
}

