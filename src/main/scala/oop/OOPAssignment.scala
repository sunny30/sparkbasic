package oop

object OOPAssignment {

  def main(args: Array[String]): Unit = {

  }
}


class Writer(firstName:String,surname:String,yearDOB:Int){

  def fullname():String={
    s"${this.firstName} ${this.surname}"
  }

  def getDOBYear():Int={
    yearDOB
  }

}


class Novel(name:String,yor:Int,author:Writer){

  def getAuthorAge():Int={
    if(yor<author.getDOBYear())
      0
    else
      yor-author.getDOBYear()
  }

  def isWrittenBy():String={
    author.fullname()
  }

  def copy(yearOfNewRealease:Int):Novel={
    new Novel(this.name,yearOfNewRealease,author)
  }
}
