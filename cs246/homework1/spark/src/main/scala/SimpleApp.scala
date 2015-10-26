import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
    def parse(str: String): List[((Int, Int), Int)] = {
        val strSplit = str.split('\t')
        val user = strSplit(0).toInt
        if (strSplit.length <= 1)
            return List(((user, user), 0))
        val friends = strSplit(1).split(',')
        val inter = for {
            friend <- friends
        } yield ((user, friend.toInt), 0)
  	
        val intra = for {
            u1 <- friends
            u2 <- friends
            if (u1.toInt != u2.toInt)
        } yield ((u1.toInt, u2.toInt), 1)
  	
        inter.toList ::: intra.toList
    }

    def pickFriends(friends: List[(Int, Int)]): List[Int] = {
        friends.sortBy(_._2).reverse.take(10).map(_._1)
    }  

    def main(args: Array[String]) {
        val logFile = "/home/cloudera/cs246/homework1/soc-LiveJournal1Adj.txt"
        val conf = new SparkConf().setAppName("Simple Application")
        val sc = new SparkContext(conf)
        val logData = sc.textFile(logFile).cache()
        val parsed = logData.flatMap(line => parse(line))
        val comFriends = parsed.reduceByKey((a, b) => a + b)
        val oldFriends = parsed.reduceByKey((a, b) => a * b)
        val posFriends = (comFriends.join(oldFriends)).
                          filter(rec => rec._2._2 != 0).
                          map(rec => (rec._1._1, (rec._1._2, rec._2._1)))
        val friendsList = posFriends.groupByKey().map(rec => (rec._1, pickFriends(rec._2.toList)))
        friendsList.collect() foreach println 
    }
}
