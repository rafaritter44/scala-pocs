import redis.clients.jedis.Jedis
import redis.clients.jedis.params.ZRangeParams

@main def hello(): Unit =
  val jedis = new Jedis()
  val key = "game:leaderboard"

  jedis.zadd(key, 70, "Alice")
  jedis.zadd(key, 100, "Bob")
  jedis.zadd(key, 90, "Charlie")

  jedis.zincrby(key, 5, "Alice")

  val top3 = jedis.zrangeWithScores(key, ZRangeParams.zrangeParams(0, 2).rev())
  println(top3) // [[Bob,100.0], [Charlie,90.0], [Alice,75.0]]

  val getRank = (player: String) => jedis.zrevrank(key, player) + 1
  val bobRank = getRank("Bob")
  println(bobRank) // 1

  jedis.del(key)
  jedis.close()