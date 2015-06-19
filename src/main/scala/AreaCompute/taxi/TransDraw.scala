package AreaCompute.taxi

import java.text.SimpleDateFormat

/**
 * Tran数据格式
 */
class TransDraw(trans: String) extends Serializable {
  def StringToLong(time: String): Long = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    (formatter.parse(time).getTime - formatter.parse("2014-01-01 00:00:00").getTime) / 1000
  }

  private val t = trans.split(",")
  val carId = t(0)
  val uptime = t(1)
  val downtime = t(2)
  val mileage = t(4)
  val money = t(6)
  /*val carId = t(0) //车牌号
  val uptime = t(2).split("T")(0) + " " + t(2).split("T")(1).substring(0, 8) //上车时间
  val downtime = t(3).split("T")(0) + " " + t(3).split("T")(1).substring(0, 8) //下车时间
  val mileage = t(5).toDouble / 1000 //交易里程
  val money = t(7).toDouble / 100 //交易金额*/
  val downtimestamp = StringToLong(downtime)
  //4LZ4Z,2014-10-01,2014-10-01T22:12:48.000Z,2014-10-01T22:24:13.000Z,240,4813,00小时01分35秒,1840,49
  //车牌号，下车时间，交易里程，交易金额
}