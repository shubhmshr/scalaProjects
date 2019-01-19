object test {
  def main(args:Array[String]) {

    val params = Map(
      "fileName_" -> args(0),
      "arg2" -> args(1)
    )

    println(params.getOrElse("arg1",0))
  }

}