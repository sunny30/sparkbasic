object PatternMatching1 {

  def main(args: Array[String]): Unit = {

    matchIt(10)
    println(matchString("bye"))
    println(opposit2("sane"))
  }


  def matchIt(x:Any): Unit = {

    x match {
      case 10 => println("value of x is 10")
      case "hello" => println("value is hello")
      case _ => println("not matched")
    }
  }

  def matchString(s:String):String = {
    s match {
      case "hello"  => "hi"
      case "bye"    => "bbye"
      case _        => "None"
    }
  }

  def opposit2(str:String) : String ={
    str match {
      case "hot" => "cold"
      case "summer" => "winter"
      case inWord @("sane" | "edible" | "secure") => s"in$inWord"
      case _ => s"Not $str"
    }

  }



}
