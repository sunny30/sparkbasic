package oop

object CaseClasses extends App {

  case class Person(name:String,age:Int)

  val p1 = new Person("Sharad",33)
  println(p1.name+" "+p1.age)

  val p2 = new Person("Sharad",33)

  println(p1==p2)
  println(p1)

  val p3 = p1.copy()
  println(p3)
  val p4 = Person("Sunny",33)

}
