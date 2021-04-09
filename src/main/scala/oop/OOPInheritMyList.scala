package oop

abstract class OOPInheritMyList {
    def isEmpty:Boolean
    def add(beg:Int):OOPInheritMyList
    def head:Int
    def tail:OOPInheritMyList
    def printElements:String
    override def toString: String={
      "["+printElements+"]"
    }
}



object Empty extends OOPInheritMyList{
  override def head: Int = ???

  override def isEmpty: Boolean = true

  override def add(beg: Int): OOPInheritMyList = ???
  override def tail:OOPInheritMyList = ???

  override def printElements: String = ""


}


class NonEmpty(h:Int,t:OOPInheritMyList) extends OOPInheritMyList{
  override def isEmpty: Boolean = false

  override def tail: OOPInheritMyList = t

  override def head: Int = h

  override def add(beg: Int): OOPInheritMyList ={
    new NonEmpty(beg,this)
  }

  override def printElements(): String = {
    if(this.isEmpty)
      " "
    else
      h+" "+t.printElements
  }


}


object ListTest extends App{
  val list = new NonEmpty(1,Empty)
  val list1 = new NonEmpty(2,list)
  val list2 = list1.add(5)

  println(list2.printElements)
}