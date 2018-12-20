package org.apache.spark.szinfo

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

object RecordUtils {
  def serialize(record: Record): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(record)
    oos.close()
    stream.toByteArray
  }

  def deserialize(raw: Array[Byte]): Record = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(raw))
    val record = ois.readObject
    ois.close()
    record.asInstanceOf[Record]
  }
}
