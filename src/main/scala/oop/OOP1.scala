package oop

object OOP1{

  def main(args: Array[String]): Unit = {
    val person = new Person("Sunny",32)
    println(person.greet("Sharad"))
  }
}


class Person(name:String,age:Int=0){

  def greet(name:String):Unit={
    println(s"Hi i am ${this.name},thanks to $name")
  }

  def this(name:String) = this(name:String,age=0)
}


