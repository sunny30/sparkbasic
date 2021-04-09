package advscala

object partialfunction extends App {

  val afun = (x:Int)=>x+1//Function[Int,Int]===Int=>Int

  val partialFun : PartialFunction[Int,Int]={
    case 1 => 42
    case 2 => 56
    case 5 => 99
    //case a => throw new RuntimeException(s"$a is not allowed")
  }

  println(partialFun(2))
 // println(partialFun(9))
  //.isdefinedAt
  println(partialFun.isDefinedAt(67))

  //.lift
  val lifted = partialFun.lift
  println(lifted(2).getOrElse(-1))
  println(lifted(99).getOrElse(-1))

  //fallback or else
  val pfchain = partialFun.orElse[Int,Int]{
    case 45 => 67
  }

  println(pfchain(45))
  println(pfchain(2))
  println(pfchain.isDefinedAt(45))

  //PF can only one parameter type
  val partialFunInstance = new PartialFunction[Int,Int] {
    override def  apply(v1: Int): Int = {
      v1 match {
        case 1=> 42
        case 2=> 56
        case 3=>99
      }
    }

    override def isDefinedAt(x: Int): Boolean = {
      x match {
        case 1=> true
        case 2=> true
        case 3=>true
        case _ => false
      }
    }
  }

  println(partialFunInstance(1))


  def ff(num:Int,num1:Int)(num2:Int,num3:Int):Int={
    (num+num1)*(num2+num3)
  }

  println(ff(2,4)(1,5))
}
