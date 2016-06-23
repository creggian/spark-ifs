package creggian.lib

import org.apache.spark._

/*
 * args(0): input file
 * args(1): output file
 */

object Transpose {
        def main(args: Array[String]) {

                val sparkConf = new SparkConf().setAppName("Transpose")
                val sc = new SparkContext(sparkConf)

		val hIdx = 0
		val separator = ","
		val inputFile = args(0)
		val outputFile = args(1)

                val data = sc.textFile(inputFile).collect
		val arr = data.map(x => x.split(separator))
		val classes = arr.map(x => x(hIdx)).filter(x => x != "class").map(x => x.toInt).toSeq.mkString(",")

		val vstring = arr.map(x => x.take(hIdx) ++ x.drop(hIdx+1))
		val transposed = vstring.toSeq.transpose.map(x => x.mkString(","))

		sc.parallelize(transposed, 1).saveAsTextFile(outputFile)
		sc.parallelize(List(classes), 1).saveAsTextFile(outputFile+"_class")
        }
}
