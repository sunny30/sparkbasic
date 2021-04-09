//class Rational(val num:Int,val den:Int) {
//
//
//  def +(other:Rational):Rational={
//    new Rational((this.num*other.den+this.den*other.num),this.den*other.den)
//
//  }
//
//  override def toString: String = {
//    s"$num/$den"
//  }
//}
//
//object Rational{
//  implicit def apply(num: Int): Rational = new Rational(num, 1)
//}
//
//object wrapper{
//  def main(args: Array[String]): Unit = {
//    val p = 5 + Rational(5)
//    println(p.toString)
//  }
//}