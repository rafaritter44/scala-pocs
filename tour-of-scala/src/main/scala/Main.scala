@main def hello(): Unit = {
  val value = "value"
  var variable = "variable value 1"
  println(value)
  println(variable)

  variable = "variable value 2"
  println(variable)

  println({
    val value = "other value"
    value + " inside block"
  })
  println(value)

  val func = (str: String) => "func called with: " + str
  println(func("hello"))
}