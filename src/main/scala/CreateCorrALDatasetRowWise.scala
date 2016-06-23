package creggian.util

import scala.util.Random
import org.apache.spark._

// args(0) nrow
// args(1) ncol
// args(2) ncorr has to be power of 2
// args(3) number of executors
// args(4) output file
// args(5) output class file
// args(6) output feature file

object CreateCorrALDatasetRowWise {
        def main(args: Array[String]) {

                val sparkConf = new SparkConf().setAppName("CAD")
                val sc = new SparkContext(sparkConf)

		val nrow = args(0).toInt
                val nrow_bc = sc.broadcast(nrow)
                val ncol_bc = sc.broadcast(args(1).toInt)
		val ncorr = args(2).toInt
                val ncorr_bc = sc.broadcast(ncorr)
                val nexecutors = args(3).toInt
                val nexecutors_bc = sc.broadcast(nexecutors)
                val outputfile = args(4)
                val outputclassfile = args(5)
                val outputfeaturefile = args(6)

                val res = sc.parallelize(1 to nexecutors).repartition(nexecutors).flatMap(x => {
                        val nrow = nrow_bc.value
                        val ncol = ncol_bc.value
                        val nexec = nexecutors_bc.value.toInt

                        // each map creates the same amount of lines
                        for (row_i <- 1 to nrow/nexec) yield {
                                val rvector = Array.fill(ncol){scala.util.Random.nextInt(2)}
                                val label = Random.alphanumeric.take(20).mkString("")

                                label.toString+","+rvector.mkString(",")
                        }
                })

                val count = res.count
                println("DEBUG: count = " + count)

                res.saveAsTextFile(outputfile)

		val res2 = sc.textFile(outputfile)
		val sam = res2.sample(false, ncorr.toDouble/nrow.toDouble).persist
		val step1 = sam.flatMap(x => {
			val s = x.split(",")
			val label = s(0)
			val v = s.drop(1).map(y => y.toInt) // drop the label
			
			v.zipWithIndex.map(y => {
				val idx = y._2
				val value = (label, y._1)
				(idx, value)
			})
		})
		val csv = step1.groupByKey.map(y => {
			val nc = ncorr_bc.value
			
			val idx = y._1
			val value = y._2.toArray
			val rvector = value.sortBy(_._1).map(y => y._2) // values sorted by label, so I have the same order in every reducer
			
			val colcorr = rvector.slice(0, nc) // upper bound
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
                        val classLabel = v_i(0)

			(idx, classLabel)
		}).repartition(1).sortBy(_._1).map(_._2).collect.toArray.mkString(",")

                sc.parallelize(List(csv), 1).saveAsTextFile(outputclassfile)

		val featureNames = sam.map(x => {
			val s = x.split(",")
			(1, s(0)) // 1 doesn't matter, I need it to create a tuple to use sortBy
		}).repartition(1).sortBy(_._2).map(_._2)

                featureNames.saveAsTextFile(outputfeaturefile)
        }
}
