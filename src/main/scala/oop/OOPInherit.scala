package oop

object OOPInherit {

  def main(args: Array[String]): Unit = {
    val dev:Developers = new SparkDevlopers(10)
    println(dev.isType())
    println(dev.category())
  }
}

class Person1(name:String,age:Int){
  val type1 = "Person"

  def isVoter:Boolean={
    age>18
  }


}


class Voter(name:String,age:Int,IdCard:String) extends Person1(name,age){

  override val type1 = "voter"

  override def isVoter:Boolean={
    true
  }



}

abstract class Developers(yoe:Int){

  def isType():String={
    if(yoe<3)
      "Junior"
    else if(yoe<=8)
      "mid-level"
    else
      "Senior"
  }

   def category():String
}

class SparkDevlopers(yoe:Int) extends Developers(yoe){
  override def category(): String = {
    "Big-data"
  }
}