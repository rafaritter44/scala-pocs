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

  val func = (arg: String) => "func called with: " + arg
  println(func("hello"))

  def method(arg: String): String = "method called with: " + arg
  println(method("hello"))
}