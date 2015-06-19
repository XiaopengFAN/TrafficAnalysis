package org.traffic.data.analysis.problem

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
 * 指定区域内交易数据计算 输出结果为商圈名+交易数+交易金额+交易里程
 */
class AreaCompute(area:RDD[String],transInfo:RDD[String]) extends Serializable{
  class Point (var x :Double , var y:Double ) extends Serializable{
    var money:Double = _
    var mileage:Double =_
    def this(x:Double,y:Double,mileage:Double,money:Double){
      this(x,y)
      this.money = money
      this.mileage = mileage
    }

    def equal(p : Point): Boolean = {
      var point: Point  = p
      if (this.x == point.x && this.y == point.y){
        true
      }else{
        false
      }
    }
  }
  //定义区域
  class Polygon(var points: ArrayBuffer[Point], var area_name: String) extends Serializable {
    //var points : Array[Point] = pointArray
    var xMin: Double = Double.MaxValue
    var xMax: Double = Double.MinValue
    var yMin: Double = Double.MaxValue
    var yMax: Double = Double.MinValue
    var count: Int = points.size
    for (p <- this.points) {
      if (p.x > this.xMax)
        this.xMax = p.x
      if (p.x < this.xMin)
        this.xMin = p.x
      if (p.y > this.yMax)
        this.yMax = p.x
      if (p.y < this.yMin)
        this.yMin = p.y
    }

    //区域name
    def getName: String = {
      this.area_name
    }

    //是否闭合
    def closure: Boolean = {
      val startPoint: Point = this.points(0)
      val endPoint: Point = this.points(this.points.size - 1)
      if (startPoint.equal(endPoint)) {
        true
      } else {
        false
      }
    }

    //是否区域是否包含点
    def contains(p: Point): Boolean = {
      if (p.x >= this.xMax || p.x < this.xMin || p.y >= this.yMax || p.y < this.yMin) {
        false
      } else {
        var cn: Int = 0
        var n: Int = this.points.size
        for (i <- 0 to n - 2) {
          if ((points(i).y != this.points(i + 1).y) &&
            !((p.y < this.points(i).y) && (p.y < this.points(i + 1).y)) &&
            !((p.y > this.points(i).y) && (p.y > this.points(i + 1).y))) {
            var uy: Double = 0
            var by: Double = 0
            var ux: Double = 0
            var bx: Double = 0
            var dir: Int = 0
            if (this.points(i).y > this.points(i + 1).y) {
              uy = this.points(i).y
              by = this.points(i + 1).y
              ux = this.points(i).x
              bx = this.points(i + 1).x
              dir = 0
            } else {
              uy = this.points(i + 1).y
              by = this.points(i).y
              ux = this.points(i + 1).x
              bx = this.points(i).x
              dir = 0
            }
            var tx: Double = 0
            if (ux != bx) {
              val k = (uy - by) / (ux - bx)
              val b = ((uy - k * ux) + (by - k * bx)) / 2
              tx = (p.y - b) / k
            } else {
              tx = ux
            }
            if (tx > p.x) {
              if (dir == 1 && p.y != this.points(i + 1).y) {
                cn = cn + 1
              } else if (p.y != this.points(i).y) {
                cn = cn + 1
              }
            }
          }
        }
        if (cn % 2 == 0)
          false
        else
          true
      }
    }
  }

  //将区域坐标文件转化为key-value键值对，key为区域名，value为区域坐标
  def divide_area(line: String): (String, ArrayBuffer[Point]) = {
    val s = line.split(" ")
    val key = s(0)
    val point_tmp: Point = new Point(s(2).toDouble, s(3).toDouble)
    var value = new ArrayBuffer[Point]()
    value += point_tmp
    (key, value)
  }

  //将gps和trans数据join后的结果转化为Point gps为坐标，trans(0)交易数，tran(1)交易金额，tran(2)交易里程
  def getPoint(line:String):Point={
    val s = line.split(";")
    val ss = s(2).split(",")
    val sss = s(3).split(",")
    val value :Point = new Point(ss(1).toDouble,ss(2).toDouble,sss(0).toDouble,sss(1).toDouble)
    value
  }

  //所有区域
  def areasToArray(areas: Array[(String, ArrayBuffer[Point])]): Array[Polygon] = {
    val areas_buffer: ArrayBuffer[Polygon] = new ArrayBuffer[Polygon]()
    for (i <- 0 until areas.length) {
      val area = new Polygon(areas(i)._2, areas(i)._1)
      areas_buffer += area
    }
    areas_buffer.toArray
  }

  //汇总交易数
  def judge_area_num(p: Point, areas: Array[Polygon]): Array[(String, Int)] = {
    val result = ArrayBuffer[(String, Int)]()
    for (i <- 0 until areas.length) {
      if (areas(i).contains(p)) {
        result += ((areas(i).getName, 1))
      }
    }
    result.toArray
  }
  //汇总交易金额
  def judge_area_money(p: Point, areas: Array[Polygon]): Array[(String, Double)] = {
    val result = ArrayBuffer[(String, Double)]()
    for (i <- 0 until areas.length) {
      if (areas(i).contains(p)) {
        result += ((areas(i).getName, p.money))
      }
    }
    result.toArray
  }
  //汇总交易里程
  def judge_area_mileage(p: Point, areas: Array[Polygon]): Array[(String, Double)] = {
    val result = ArrayBuffer[(String, Double)]()
    for (i <- 0 until areas.length) {
      if (areas(i).contains(p)) {
        result += ((areas(i).getName, p.mileage))
      }
    }
    result.toArray
  }
  def run= {
    val areaData = area.map(divide_area).reduceByKey((a, b) => a ++= b).collect
    //rdd转化为array
    val areaToArray: Array[Polygon] = areasToArray(areaData)
    //交易数
    val num = transInfo.map(getPoint).flatMap(judge_area_num(_, areaToArray)).reduceByKey(_ + _)
    //交易金额
    val money = transInfo.map(getPoint).flatMap(judge_area_money(_, areaToArray)).reduceByKey(_ + _)
    //交易里程
    val mileage = transInfo.map(getPoint).flatMap(judge_area_mileage(_, areaToArray)).reduceByKey(_ + _)
    val t1 = num.join(mileage)
    val t2 = t1.join(money)
    val result = t2.map {
      case (key, value) => {
        key + ";" + value._1._1 + ";" + value._1._2/1000 +","+value._1._2/(1000*value._1._1)+ ";" + value._2/100+ "," +value._2/(100*value._1._1)
      }
    }
    result //返回结果为:商圈名，交易数量，交易总里程，交易总金额，平均交易里程，平均交易金额
  }
}
