package oop

import scala.util.Random

object PatternMatchingEx extends App{

  val r = Random
  val rval = r.nextInt(5)

  val description = rval match {
    case 1 => "Lucky one"
    case 2 => "Double or nothing"
    case 3 => "triplet"
    case _ => "default"
  }

  println(description)


  case class Person(name:String,age:Int)

  val sunny = Person("sunny",33)
  val greeting = sunny match {
    case Person(n,a) if a>30 => s"$n here, a senior one"
    case Person(n,a) => s"$n here"
    case _ => "wrong greeting"
  }

  println(greeting)

  class Car
  case class SUV(engine:Int) extends Car
  case class Taxi(luxury:String) extends Car

  val newCar:Car = Taxi("50")

  val cid = newCar match {
    case SUV(engine) => s"Power engine value is $engine"
    case Taxi(luxury) => s"Fair ?"
    case _ => "Not known"
  }
  println(cid)

  val alist:List[Int] = List(1,2,4)

  val alid = alist match {
    case l : List[String] => "list of strings"
    case i : List[Int] => "List of Integers"
    case _ => "unknown"
  }

  println(alid)


}
