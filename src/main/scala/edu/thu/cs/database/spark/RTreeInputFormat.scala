package edu.thu.cs.database.spark

import java.io.IOException

import org.apache.hadoop.mapred.{FileSplit, InputSplit, JobConf, SequenceFileInputFormat}

/**
  * Created by lihaoda on 2016/5/9.
  */
class RTreeInputFormat[K, V] extends SequenceFileInputFormat[K, V]{

  @throws(classOf[IOException])
  override def getSplits(job: JobConf, numSplits: Int): Array[InputSplit] = {
    val splits = super.getSplits(job, numSplits)
    splits.zipWithIndex.foreach(t => {
      println(t._2)
      t._1 match {
        case fs: FileSplit => println(fs.getPath.toString)
        case a:_ => println("unknown")
      }
    })
    splits
  }
}
