object Solution {
  def calculateMinimumHP(dungeon: Array[Array[Int]]): Int = {
    val m = dungeon.length
    val n = dungeon(0).length
    
    // Create a DP table where dp[i][j] represents the minimum health required to reach the princess
    val dp = Array.ofDim[Int](m, n)
    
    // Base case: the knight's minimum health required at the princess's position
    dp(m - 1)(n - 1) = math.max(1, 1 - dungeon(m - 1)(n - 1))
    
    // Fill the last row (can only move right)
    for (j <- n - 2 to 0 by -1) {
      dp(m - 1)(j) = math.max(1, dp(m - 1)(j + 1) - dungeon(m - 1)(j))
    }
    
    // Fill the last column (can only move down)
    for (i <- m - 2 to 0 by -1) {
      dp(i)(n - 1) = math.max(1, dp(i + 1)(n - 1) - dungeon(i)(n - 1))
    }
    
    // Fill the rest of the DP table
    for (i <- m - 2 to 0 by -1) {
      for (j <- n - 2 to 0 by -1) {
        dp(i)(j) = math.max(1, math.min(dp(i + 1)(j), dp(i)(j + 1)) - dungeon(i)(j))
      }
    }
    
    // The result is the minimum initial health required at the top-left corner
    dp(0)(0)
  }
}