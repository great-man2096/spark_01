
object t6_26 {
  def main(args: Array[String]): Unit = {
    println(reverse(110))
  }
  def reverse(x:Int):Int = {
    val it = digits(x).toIterator
    var sum = 0
    var flag = true
    while (flag && it.hasNext) {
      val value = it.next()
      value match {
        case y if sum > Int.MaxValue / 10 => flag = false
        case y if sum < Int.MinValue / 10 => flag = false
        case y if sum == Int.MaxValue / 10 && y > 7 => flag = false
        case y if sum == Int.MinValue / 10 && y < -8 => flag = false
        case y => sum = sum * 10 + y
      }
    }
    if (flag) {
      sum
    } else {
      0
    }
  }
  def digits(x: Int): List[Int] = {
    var l: List[Int] = Nil
    var y = x
    while (y != 0) {
      l :+= y % 10
      y /= 10
    }
    l
  }

}
