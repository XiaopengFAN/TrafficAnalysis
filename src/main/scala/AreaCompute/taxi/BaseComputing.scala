package AreaCompute.taxi

import org.apache.spark.rdd.RDD

object BaseComputing {
  //获取车辆信息集合，转化为GPSRecord存储，按时间排序。
  def getCarInfo(data: RDD[(String, String)]) = {
    data.groupByKey().map {
      //按车牌号分组
      case (key, value) => {
        (key, value.toArray.sortBy(_.split(",")(1))) //数据去重并转化为GPSRecord对象,按照时间排序
      }
    }
  }
}
