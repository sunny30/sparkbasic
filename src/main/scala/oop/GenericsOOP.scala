package oop

object GenericsOOP extends App {

  class MyList[+A](head:A,t:MyList[A]){
    def add[B>:A](element:B):MyList[B] = new MyList[B](head,this)
  }

  object MyList{
    def emtpy[A]:MyList[A] = MyList.emtpy[A]
  }

  //variance problem

  class Animal
  class Dog extends Animal
  class cat extends Animal

  class CovariantList[+A]{

  }

  class Cage[A <: Animal](Animal:A)

  val cage = new Cage(new Dog)
}
