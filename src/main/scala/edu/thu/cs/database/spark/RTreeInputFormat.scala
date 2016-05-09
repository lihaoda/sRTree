package edu.thu.cs.database.spark

import java.io.IOException

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapred.{FileSplit, InputSplit, JobConf, SequenceFileInputFormat}

/**
  * Created by lihaoda on 2016/5/9.
  */
class RTreeInputFormat[K, V] extends SequenceFileInputFormat[K, V]{



  @throws(classOf[IOException])
  override protected def listStatus (job: JobConf):Array[FileStatus] = {
    val files = super.listStatus(job)
    files.zipWithIndex.foreach(t => {
      println(t._2)
      println(t._1.getPath.toString, t._1.getLen)
    })
    files
  }

  @throws(classOf[IOException])
  override def getSplits(job: JobConf, numSplits: Int): Array[InputSplit] = {
    val splits = super.getSplits(job, numSplits)
    splits.zipWithIndex.foreach(t => {
      println(t._2)
      t._1 match {
        case fs: FileSplit =>
          println(fs.getPath.toString, fs.getStart, fs.getLength)
        case _ => println("unknown")
      }
    })
    splits
  }
}
