package creggian.mrmr

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.util.MLUtils

import creggian.mrmr.lib.IterativeFeatureSelection

// ## common args between column-wise and row-wise
// args(0) input file
// args(1) iterative feature selection type:  either "column-wise" or "row-wise"
// args(2) nfs
// args(3) format, eg. 'csv', 'tsv', 'libsvm'
// args(4) labelIdx
// args(5) score class name (string)
// args(6) class input file

// val raw = MLUtils.loadLibSVMFile(sc, "a2a.libsvm")

object Run {
    def main(args: Array[String]) {
        this.run(args)
    }
    
    def run(args: Array[String]) {
        val sparkConf = new SparkConf().setAppName("Run")
        val sc        = new SparkContext(sparkConf)
    
        val filename       = args(0)
        val typeWise       = args(1).toString  // either "column-wise" or "row-wise"
        val nfs            = args(2).toInt
        val inputFormat    = args(3)
        val labelIdx       = args(4).toInt
        val scoreClassName = args(5)
        
        println("\nIterative Feature Selection with parameters:")
        println("-- input data:   " + filename)
        println("-- input format: " + inputFormat)
        println("-- data layout:  " + typeWise)
        println("-- features:     " + nfs)
        println("-- index label:  " + labelIdx)
        println("-- score class:  " + scoreClassName)
        if (typeWise == "row-wise") println("-- input labels: " + args(6))
    
        if (inputFormat == "csv" | inputFormat == "tsv") {
            val separator = if (inputFormat == "tsv") "\t" else ","
            val clVector  = if (typeWise == "row-wise") sc.textFile(args(6)).first().split(separator).map(_.toDouble) else null
        
            val data = sc.textFile(filename).map(x => {
                val arr = x.split(separator)
                val label = arr(labelIdx)
                val v = arr.take(labelIdx) ++ arr.drop(labelIdx + 1)
                (label, v.map(_.toDouble))
            })
            this.start(sc, data, typeWise, nfs, scoreClassName, clVector)
        }
    
        if (inputFormat == "libsvm") {
            val clVector  = if (typeWise == "row-wise") sc.textFile(args(6)).first().split(",").map(_.toDouble) else null
            
            val data = MLUtils.loadLibSVMFile(sc, filename).map(x => {
                (x.label.toInt.toString, x.features.toArray)
            })
            this.start(sc, data, typeWise, nfs, scoreClassName, clVector)
        }
    }
    
    def start(sc: SparkContext, data: RDD[(String, Array[Double])], typeWise: String, nfs: Int, scoreClassName: String, clVector: Array[Double]) {
        val ifs = new IterativeFeatureSelection()
        
        if (typeWise == "column-wise") this.printResults(ifs.columnWise(sc, data, nfs, scoreClassName))
        if (typeWise == "row-wise")    this.printResults(ifs.rowWise(sc, data, nfs, scoreClassName, clVector))
    }
    
    def printResults(res: Array[(String, Double)]) {
        println("\nSelected Features:")
        println("Order\tName\tScore")
        for (i <- 0 until res.length) {
            val idx    = i+1
            val item   = res(i)
            val vName  = item._1
            val vScore = item._2
            val v      = f"$vScore%1.3f"
            println(idx + "\t" + vName + "\t" + v)
        }
        println()
    }
}
