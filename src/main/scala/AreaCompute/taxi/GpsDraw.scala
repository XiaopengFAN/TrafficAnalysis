package AreaCompute.taxi

import java.text.SimpleDateFormat

/**
 * Gps数据格式
 */
class GpsDraw(gps: String) extends Serializable {
  def StringToLong(time: String): Long = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    (formatter.parse(time).getTime - formatter.parse("2014-01-01 00:00:00").getTime) / 1000
  }

  private val g = gps.split(",")
  val carId = g(0)
  //车牌号
  val longitude = g(1)
  //经度
  val latitude = g(2)
  //纬度
  val time = g(3)
  //时间
  val timestamp = StringToLong(time)
  //val time = g(3).substring(0,g(3).indexOf("Z")-2)//时间
  //4LZ4Z,114.128304,22.5504,2014-10-01T03:18:48.000Z,0,0,0,0
  //车牌号，经度，纬度，时间
}
