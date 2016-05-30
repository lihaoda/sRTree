package edu.thu.cs.database.spark.partitioner

import edu.thu.cs.database.spark.spatial._

/**
  * Created by lihaoda on 2016/5/5.
  */
object HilbertRecBuilder {

  // def getRTreeRecs[T <: Geometry](sampleData:Array[T], recNum:Int):Array[Rectangle] = {
  //     val mbrs:Array[Rectangle] = sampleData.map((g:Geometry)=>g.mbr())
  //     return innerGetRTreeRecs(mbrs, recNum);
  // }
  // def create[T <: Geometry](sampleData:Array[T], numPartitions:Int): RTreePartitioner = {
  //     new RTreePartitioner(getRTreeRecs(sampleData, numPartitions-1))
  // }
  private def bindNormalized(mbrs:Array[MBR]) = {
    val mx = mbrs.sortWith((s:MBR, t:MBR) => (s.low.coord(0) + s.high.coord(0)) < (t.low.coord(0) + t.high.coord(0)))
    val myn = mx.zipWithIndex.sortWith((s:(MBR, Int), t:(MBR, Int)) => (s._1.low.coord(1) + s._1.high.coord(1)) < (t._1.low.coord(1) + t._1.high.coord(1)))
    // for (m <- myn) {
    //     println(m._1.x1() + " " + m._1.x2() + " " + m._1.y1() + " " + m._1.y2())
    //     println(m._2)
    // }
    val (my, xs) = myn.unzip
    my.zip(xs.zipWithIndex)
  }
  private def hilbertSort(limit:Int)(xyi:(Int, Int)):Int = {
    var partition = limit
    var x = xyi._1
    var y = xyi._2
    var base:Int = 0
    do {
      partition /= 2
      base *= 4
      if (y >= partition) {
        if (x >= partition) {
          base += 2
          x -= partition
          y -= partition
        } else {
          base += 1
          y -= partition
        }
      } else {
        if (x >= partition) {
          base += 3
          val tmp = partition - 1 - y
          y = partition - 1 - (x - partition)
          x = tmp
        } else {
          val tmp = x;
          x = y;
          y = tmp;
        }
      }
    }while (partition > 1)
    base
  }
  private def least2power(xi:Int):Int = {
    var num:Int = 1
    var x = xi - 1
    while (x > 0) {
      x = x/2
      num = num * 2
    }
    num
  }
  def hilbertialize(mbrs:Array[MBR]) = {
    val mbrs_normalized = bindNormalized(mbrs)
    val (mbrs2, xy) = mbrs_normalized.unzip
    val hilbertSortN = hilbertSort(least2power(mbrs.length))(_)
    (mbrs2 zip xy.map(hilbertSortN)).sortWith(_._2 < _._2).unzip._1
  }
  def getRTreeRecs(mbrs:Array[Point], recNum:Int):Array[MBR] = {
    require(mbrs.forall(_.coord.length == 2))
    val hmbrs = hilbertialize(mbrs.map( a => MBR(a,a)))
    val subNum:Int = hmbrs.length / recNum + (if (hmbrs.length % recNum > 0) 1 else 0)
    hmbrs.grouped(subNum).map({
      _.reduce((a,b) => {
        MBR(
          Point(Array(
            Math.min(a.low.coord(0), b.low.coord(0)),
            Math.min(a.low.coord(1), b.low.coord(1)))
          ),
          Point(Array(
            Math.min(a.high.coord(0), b.high.coord(0)),
            Math.min(a.high.coord(1), b.high.coord(1)))
          )
        )
      })
    }).toArray
  }

}

