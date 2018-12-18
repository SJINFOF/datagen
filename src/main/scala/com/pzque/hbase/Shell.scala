package com.pzque.hbase

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.conf.Configuration
import scala.collection.JavaConverters._
import scala.io.StdIn.readLine


abstract class Command {
  def execute(): Unit
}


object Shell {
  type Row = (Array[Byte], Array[Cell])
  val help =
    """
      |Usage:
      |
      |Examples:
      |      get 7000000 201001010901
      |      scan 7000000 201001010901 201101010901
      |      show 5
      |      count
    """.stripMargin

  val conf: Configuration = HBaseConfiguration.create()
  val connection = ConnectionFactory.createConnection(conf)
  val table = connection.getTable(TableName.valueOf(Bytes.toBytes("hisdata5g")))
  var results: Iterable[Row] = Array[Row]()

  val getPattern = "\\s*get\\s+(\\d+)\\s+(\\d+)\\s*".r
  val scanPattern = "\\s*scan\\s+(\\d+)\\s+(\\d+)\\s+(\\d+)\\s*".r
  val showPattern = "\\s*show\\s+(\\d+)\\s*".r
  val showPattern1 = "\\s*show\\s*".r
  val countPattern = "\\s*count\\s*".r
  val quitPattern = "\\s*(q|quit)\\s*".r

  def main(args: Array[String]): Unit = {
    while (true) {
      val raw = readLine("> ")
      val command = parse(raw)
      command.execute()
    }
  }

  def parse(raw: String): Command = {
    raw match {
      case getPattern(code, time) => QGet(code, time)
      case scanPattern(code, start, stop) => QScan(code, start, stop)
      case showPattern(n) => Show(n.toInt)
      case showPattern1() => Show()
      case countPattern() => Count()
      case quitPattern(q) => Quit()
      case x => Unknown(x)
    }
  }

  def timer[T](name: String)(f: => T): T = {
    val start = System.currentTimeMillis()
    val ret = f
    val end = System.currentTimeMillis()
    println(f"Operation '$name' cost ${end - start} ms")
    ret
  }

  case class QScan(code: String, start: String, stop: String) extends Command {

    override def execute(): Unit = {
      val rowStart = f"$code$start"
      val rowStop = f"$code$stop"
      val scan = new Scan()
        .withStartRow(Bytes.toBytes(rowStart))
        .withStopRow(Bytes.toBytes(rowStop))
        .setCaching(87600)
      val scanner = table.getScanner(scan)

      results = timer("scan") {
        scanner.asScala.map(x => result2row(x))
      }

      println(f"Got ${results.size} records.")
    }
  }

  case class QGet(code: String,time:String) extends Command {
    override def execute(): Unit = {
      val key = f"$code$time"
      val get = new Get(Bytes.toBytes(key))
      val ret = timer("get") {
        table.get(get)
      }
      results = Array(result2row(ret))
    }
  }

  case class Show(n: Int = 10) extends Command {
    override def execute(): Unit = {
      var i = 1
      for (row <- results) {
        printRow(row)
        if (i > n) {
          return
        }
        i += 1
      }
    }
  }

  case class Count() extends Command {
    override def execute(): Unit = {
      println(results.size)
    }
  }

  case class Quit() extends Command {
    override def execute(): Unit = {
      table.close()
      connection.close()
      System.exit(0)
    }
  }

  case class Unknown(str: String) extends Command {
    override def execute(): Unit = {
      println(f"Unknown command sequence '$str'")
      println(help)
    }
  }

  def result2row(result: Result): Row = {
    val key = result.getRow
    val cells = result.rawCells()
    (key, cells)
  }

  def printRow(row: Row) = {
    val (key, cells) = row
    print(Bytes.toString(key) + " : ")
    for (cell <- cells) {
      val col_name = Bytes.toString(CellUtil.cloneQualifier(cell))
      val col_value = Bytes.toString(CellUtil.cloneValue(cell))
      print(f"$col_name:$col_value,")
    }
    println()
  }

}
