object Main {
  def main(args: Array[String]): Unit = {
    val dungeon1 = Array(
      Array(-2, -3, 3),
      Array(-5, -10, 1),
      Array(10, 30, -5)
    )

    val dungeon2 = Array(
      Array(0)
    )

    val result1 = Solution.calculateMinimumHP(dungeon1)
    println(s"Minimum HP needed for dungeon1: $result1")

    val result2 = Solution.calculateMinimumHP(dungeon2)
    println(s"Minimum HP needed for dungeon2: $result2")
  }
}
