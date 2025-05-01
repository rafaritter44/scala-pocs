object Main {
  def main(args: Array[String]): Unit = {
    val dungeon = Array(
      Array(-2, -3, 3),
      Array(-5, -10, 1),
      Array(10, 30, -5)
    )

    val result = Solution.calculateMinimumHP(dungeon)
    println(s"Minimum HP needed: $result")
  }
}
