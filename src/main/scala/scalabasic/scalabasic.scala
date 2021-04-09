package scalabasic

object scalabasic {
  def main(args: Array[String]): Unit = {
    println(5)
    val aCondition = true
    val assignVal = if(aCondition)3 else 5
    println(assignVal)

    //code block example
    val codeblockAssignVal = {
      val y =2
      val z = y+1
      if(y>z)
        "matched"
      else
        "unmatched"
    }

    println(codeblockAssignVal)

    callByValue(System.nanoTime())
    callByName(System.nanoTime())

    println(factorial(5))
    println(nativeStringUtil("Hello Sharad"))
  }


  def callByValue(time:Long):Unit={
    println("callbyValue "+time)
    println("callbyValue "+time)
  }

  def callByName(time: =>Long):Unit={
    println("callByName "+time)
    println("callByName "+time)
  }

  def factorial(n:Int,acc:Int=1):Int={
    if(n<=1)
      acc
    else
      factorial(n-1,n*acc)
  }

  def nativeStringUtil(str:String):String={
    str.split(" ").toSeq.map(x=>x.concat("")).mkString("|")
  }




}
