package creggian.lib

import org.apache.spark._

/*
 * args(0): input file
 * args(1): number of partition (if < 0 then repartition = nrow)
 * args(2): output file
 */

object Repartition {
        def main(args: Array[String]) {

                val sparkConf = new SparkConf().setAppName("Repartition")
                val sc = new SparkContext(sparkConf)

                val data = sc.textFile(args(0))
                val datarep = data.repartition(args(1).toInt)
                datarep.saveAsTextFile(args(2))
        }
}

