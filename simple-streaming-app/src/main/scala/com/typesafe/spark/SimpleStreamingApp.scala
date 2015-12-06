package com.typesafe.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.rate.PIDRateEstimator
import org.apache.spark.streaming.Milliseconds
import scala.util.Try
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import com.typesafe.spark.test.Hanoi
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import scopt.OptionParser
import scopt.Zero
import com.typesafe.spark.rs.TcpPublisher
import com.typesafe.spark.rs.SubscriberInputDStream
import org.reactivestreams.Publisher

/**
 * Simple statistics data class
 */
private case class Stats(count: Int, sum: Long, mean: Double, stdDev: Double, millis: Long)

object SimpleStreamingApp {

  def main(args: Array[String]): Unit = {

    // TODO: have different hostnames too

    val config = parseArgs(args)

    {
      import config._
      println(s"""
        Connecting to $hostname:${ports.mkString(",")}:${reactivePorts.mkString(",")} with ${if (pid) "PID" else "no PID"} using Spark $master.
      """)
    }
    val conf = new SparkConf()
      .setAppName("Streaming tower of Hanoi resolution")
      .set("spark.executor.memory", "512m")

    if (config.pid) {
      conf.set("spark.streaming.backpressure.enabled", "true")
    }

    if (conf.getOption("spark.master").isEmpty)
      conf.setMaster(config.master)

    val ssc = new StreamingContext(conf, Milliseconds(config.batchInterval))

    val tcpStreams = config.ports.map { p =>
      ssc.socketTextStream(config.hostname, p, StorageLevel.MEMORY_ONLY)
    }

    val rsStreams = config.reactivePorts.map { p =>
      new SubscriberInputDStream(ssc, StorageLevel.MEMORY_ONLY)({
        val h = config.hostname
        val po = p
        val f = () => new TcpPublisher(h, po)
        f
      })
    }

    val allStreams = tcpStreams ::: rsStreams

    val computedSTreams = allStreams.map { lines =>
      val streamId = lines.id

      val numbers = lines.flatMap { line => Try(Integer.parseInt(line)).toOption }

      val hanoiTime = numbers.map { i =>
        // keep track of time to compute
        val startTime = System.currentTimeMillis()

        // resolve the tower of Hanoi
        Hanoi.solve(i)

        val executionTime = System.currentTimeMillis() - startTime
        (i, executionTime)
      }
      hanoiTime.groupByKey().mapValues { stats }.map(t => (t._1, streamId, t._2))

    }

    val allResults = computedSTreams.reduce(_ union _)

    allResults.foreachRDD { (v, time) =>
      if (!v.isEmpty()) {
        v.collect.foreach(s => println(format(time.milliseconds, s._1, s._2, s._3)))
      }
    }

    ssc.start()

    Future {
      while (Console.readLine() != "done") {

      }
      ssc.stop(true)
    }

    ssc.awaitTermination()
    System.exit(0)
  }

  private def format(batchTime: Long, value: Int, streamId: Int, stats: Stats): String = {
    s"batch result: ${stats.millis}\t$batchTime\t$value\t$streamId\t${stats.count}\t${stats.sum}\t${stats.mean}\t${stats.stdDev}"
  }

  /**
   * Returns count, sum, mean and standard deviation
   *
   */
  private def stats(value: Iterable[Long]): Stats = {
    if (value.isEmpty) (0, 0L, 0D, 0D, System.currentTImeMillis())
    else {
      // rounding error-free computation of variance
      // see http://bit.ly/1CfmaiE
      val (count, sum, mean, S) = value.tail.foldLeft(0, 0L, value.head.toDouble, 0D) {(acc,v) =>
        val nextMean = acc._3 + (v – acc._3)/k
        (acc._1 + 1, acc._2 + v, nextMean, acc._4 + (v – acc._3) * (v – nextMean))
      }
      Stats(count, sum, mean, math.sqrt(S / (count - 1)), System.currentTimeMillis())
    }
  }

  val DefaultConfig = Config("local[*]", "", Nil, false, Nil, 1000, "ignore", 1)

  private val parser = new OptionParser[Config]("simple-streaming") {
    help("help")
      .text("Prints this usage text")

    opt[String]('h', "hostname")
      .required()
      .action { (x, c) => c.copy(hostname = x) }
      .text("Hostname where receivers should connect")

    opt[Seq[Int]]('p', "ports")
      .optional()
      .action { (x, c) => c.copy(ports = x.to[List]) }
      .text("Port number to which the TCP receivers should connect")

    opt[Unit]('i', "pid")
      .action { (_, c) => c.copy(pid = true) }
      .text("Enable the PID rate estimator")

    opt[Seq[Int]]('r', "reactivePorts")
      .optional()
      .action { (x, c) => c.copy(reactivePorts = x.to[List]) }
      .text("Port number to which the RS receivers should connect")

    opt[String]('m', "master")
      .action { (x, c) => c.copy(master = x) }
      .text("Spark master to connect to")

    opt[Int]('b', "batch-interval")
      .action { (x, c) => c.copy(batchInterval = x) }
      .text("The batch interval in milliseconds.")

  }

  private def parseArgs(args: Array[String]): Config = {
    parser.parse(args, DefaultConfig) match {
      case Some(config) =>
        // checks that at least one port is defined
        if (config.ports.isEmpty && config.reactivePorts.isEmpty) {
          parser.reportError("'ports' or 'reactivePorts' should be defined")
          sys.exit(1)
        }
        config
      case None =>
        sys.exit(1)
    }
  }
}
