package com.pzque.hbase

import java.io.{File, PrintWriter}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, Month}

import scala.util.Random


case class Record(SecurityID: String,
                  DateTime: String,
                  PreClosePx: Float,
                  OpenPx: Float,
                  HighPx: Float,
                  LowPx: Float,
                  LastPx: Float,
                  Volume: Float,
                  Amount: Float,
                  IOPV: Float) {
  def rowkey(): String = {
    f"$SecurityID$DateTime"
  }

  override def toString: String =
    f"${rowkey()},$SecurityID,$DateTime,$PreClosePx,$OpenPx,$HighPx,$LowPx,$LastPx,$Volume,$Amount,$IOPV"
}

object RandomRecord {

  val minutesList = {
    901.to(1100).map(x => f"$x%04d").filter(x => x.slice(2, 4).toInt < 60) ++
      1301.to(1500).map(x => f"$x%04d").filter(x => x.slice(2, 4).toInt < 60)
  }

  var start: LocalDate = LocalDate.of(2008, Month.JANUARY, 1)
  var current: LocalDate = start
  var id: String = "700000"

  def setStart(datetime: LocalDate): RandomRecord.type = {
    start = datetime
    current = datetime
    this
  }

  def setID(id: String): RandomRecord.type = {
    this.id = id
    current = start
    this
  }

  def nextDay(): List[Record] = {
    val day = current
    val ret = minutesList.map(
      min => f"${day.format(DateTimeFormatter.ofPattern("yyyyMMdd"))}$min"
    ).map(datetime => randomRecord(id, datetime)).toList

    current = current.plusDays(1)
    ret
  }

  private def randomRecord(securityID: String, dateTime: String): Record = {
    val Array(preClosePx, openPx, highPx, lowPx, lastPx, volume, amount, iOPV) = randomArray
    Record(securityID, dateTime, preClosePx, openPx, highPx, lowPx, lastPx, volume, amount, iOPV)
  }

  private def randomArray: Array[Float] = {
    Array.fill(8)(Random.nextFloat)
  }
}

object DataGen {

  def main(args: Array[String]): Unit = {

    val writer = new PrintWriter(new File("data.csv"))

    for (n <- 7000000 to 7000300) {
      println(f"Generating data for $n...")

      val id = n.toString

      RandomRecord.setID(id)

      val days = List.fill(365 * 10)(RandomRecord.nextDay).flatten

      println(f"Writing data for $n...")

      for (d <- days) {
        writer.println(d)
      }
    }
    writer.close()
  }
}

