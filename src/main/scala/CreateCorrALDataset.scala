package creggian.util

import org.apache.spark._

// args(0) nrow
// args(1) ncol
// args(2) ncorr has to be power of 2
// args(3) number of executors
// args(4) number of iteration (loop)
// args(5) output file

object CreateCorrALDataset {
        def main(args: Array[String]) {

                val sparkConf = new SparkConf().setAppName("CAD")
                val sc = new SparkContext(sparkConf)

		val nrow_bc = sc.broadcast(args(0).toInt)
		val ncol_bc = sc.broadcast(args(1).toInt)
		val ncorr_bc = sc.broadcast(args(2).toInt)
		val nexecutors = args(3).toInt
		val nexecutors_bc = sc.broadcast(nexecutors)
		val numIter = args(4).toInt
		val outputfile = args(5)

		var tot : org.apache.spark.rdd.RDD[String] = sc.emptyRDD
		for (k <- 1 to numIter) {
			val res = sc.parallelize(1 to nexecutors).repartition(nexecutors).flatMap(x => {
				val nrow = nrow_bc.value.toInt
				val ncol = ncol_bc.value.toInt
				val ncorr = ncorr_bc.value.toInt
				val nexec = nexecutors_bc.value.toInt

				// each map creates the same amount of lines
				for (row_i <- 1 to nrow/nexec) yield {
					val rvector = Array.fill(ncol){scala.util.Random.nextInt(2)}
					val colcorr = rvector.slice(0, ncorr)

					val niter = (scala.math.log(colcorr.size) / scala.math.log(2)).toInt

					var v_i = colcorr
					for (i <- 1 to niter) {
						val ncol_i = (v_i.size).toInt

						val odd = v_i.zipWithIndex.filter(_._2 % 2 == 1).map(_._1)
						val even = v_i.zipWithIndex.filter(_._2 % 2 == 0).map(_._1)

						v_i = for (j <- (0 until odd.size).toArray) yield {
							var ret = -1
							if (i % 2 == 1) {
								ret = odd(j) & even(j)
							} else {
								ret = odd(j) | even(j)
							}
							ret
						}
					}
					val label = v_i(0)

					label.toString+","+rvector.mkString(",")
				}
			})

			val c = res.count // force execution of data

			tot = tot.union(res)
		}

		val count = tot.count
		println("DEBUG: count = " + count)

		tot.saveAsTextFile(outputfile)
        }
}

