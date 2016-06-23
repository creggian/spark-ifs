package creggian.mrmr

import java.util.Iterator
import org.apache.spark._
import scala.collection.JavaConversions._

import creggian.mrmr.lib.MRMR

// ## common args between column-wise and row-wise
// args(0) input file, eg /user/creggian/public/test_lymphoma_s3_noheader.csv
// args(1) mRMR type:  either "column-wise" or "row-wise"
// args(2) nfs
// args(3) comma-separated class levels, eg 0,1,2,3,4
// args(4) comma-separated features levels, eg -2,0,2
// args(5) csv separator, eg ,

object Run {
        def main(args: Array[String]) {

                val sparkConf = new SparkConf().setAppName("Run")
                val sc = new SparkContext(sparkConf)

		val filename = args(0)
		val csv = sc.textFile(filename)
                val typeWise = args(1).toString // either "column-wise" or "row-wise"
                val nfs = args(2).toInt

		val separator = args(5)
		val classLevels = args(3).split(separator).map(_.toDouble)
		val featuresLevels = args(4).split(separator).map(_.toDouble)

		val mrmr = new MRMR()

		if (typeWise == "column-wise") {

			// args(6) classIdx index of the class column (if column wise)

			println("mRMR column-wise operation selected")

			// 'v' variable for column-wise mRMR
			val classIdx = args(6).toInt // column idx of the label or an Array of integer (0 is the first column's index !!)
			val v = csv.map(x => x.split(separator).map(y => y.toDouble)) // org.apache.spark.rdd.RDD[Array[Double]]
			val rdd = v // RDD[Array[Double]]

			mrmr.columnWise(sc, rdd, nfs, classLevels, featuresLevels, classIdx)
		}
		if (typeWise == "row-wise") {
			println("mRMR row-wise operation selected")

			// args(6) headerIdx index of the header column, eg. 0
			// args(7) input file with csv class vector
			// args(8) score class, e.g. creggian.mrmr.feature.common.Score

			val separator_bc = sc.broadcast(separator)

			val headerIdx = args(6).toInt
			val headerIdx_bc = sc.broadcast(headerIdx)
			val classVectorInputFile = args(7)
			val scoreClassName = args(8)
			
			val clVector = sc.textFile(classVectorInputFile).collect.toArray.flatMap(x => x.split(separator).map(y => y.toDouble))

			val rdd = csv.map(x => {
				val hIdx = headerIdx_bc.value
				
				val arr = x.split(separator_bc.value)
				val variableLabel = arr(hIdx)
				val vstring = arr.take(hIdx) ++ arr.drop(hIdx+1)
				val v = vstring.map(x => x.toDouble)

				(variableLabel, v)
			})

			mrmr.rowWise(sc, rdd, nfs, classLevels, featuresLevels, clVector, scoreClassName)
		}
	}
}
