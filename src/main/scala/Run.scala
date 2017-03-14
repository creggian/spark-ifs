package creggian.mrmr

import org.apache.spark._
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

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
        val ecl = new EntryCommandLine()
        ecl.run(args)
    }
}
    
class EntryCommandLine {
    def run(args: Array[String]) {
        val sparkConf = new SparkConf().setAppName("Iterative Feature Selection")
        val sc        = new SparkContext(sparkConf)
    
        val filename       = args(0)
        val typeWise       = args(1).toString  // either "column-wise" or "row-wise"
        val nfs            = args(2).toInt
        val inputFormat    = args(3)
        val labelIdx       = args(4).toInt
        val scoreClassName = args(5)
    
        val nPartition     = sc.getConf.getInt("spark.default.parallelism", 50)
        
        println("\nIterative Feature Selection with parameters:")
        println("-- input data:    " + filename)
        println("-- input format:  " + inputFormat)
        println("-- data layout:   " + typeWise)
        println("-- features:      " + nfs)
        println("-- index label:   " + labelIdx)
        println("-- score class:   " + scoreClassName)
        if (typeWise == "row-wise") println("-- input labels:  " + args(6))
        println("-- num partition: " + nPartition)
    
        if (inputFormat == "csv" | inputFormat == "tsv") {
            val separator = if (inputFormat == "tsv") "\t" else ","
            val clVector  = if (typeWise == "row-wise") sc.textFile(args(6)).first().split(separator).map(_.toDouble) else null
        
            val data = sc.textFile(filename).map(x => {
                val arr = x.split(separator)
                val label = arr(labelIdx).toDouble
                val v = arr.take(labelIdx) ++ arr.drop(labelIdx + 1)
                val dv = new DenseVector(v.map(_.toDouble))
                new LabeledPoint(label, dv.compressed)
            })
    
            val rdd = data.repartition(numPartitions = nPartition).persist(StorageLevel.MEMORY_AND_DISK_SER)
    
            this.start(sc, rdd, typeWise, nfs, scoreClassName, clVector)
        }
    
        if (inputFormat == "libsvm") {
            val data = MLUtils.loadLibSVMFile(sc, filename).map(lp => new LabeledPoint(lp.label, lp.features.compressed))
            
            val rdd = data.repartition(numPartitions = nPartition).persist(StorageLevel.MEMORY_AND_DISK_SER)
            
            val clVector  = if (typeWise == "row-wise") sc.textFile(args(6)).first().split(",").map(_.toDouble) else null
            this.start(sc, rdd, typeWise, nfs, scoreClassName, clVector)
        }
    }
    
    def start(sc: SparkContext, data: RDD[(LabeledPoint)], typeWise: String, nfs: Int, scoreClassName: String, clVector: Array[Double]) {
        val ifs = new IterativeFeatureSelection()
        
        if (typeWise == "column-wise") this.printResults(ifs.columnWise(sc, data, nfs, scoreClassName))
        if (typeWise == "row-wise")    println("\n!!! Operation not yet implemented !!!\n") // this.printResults(ifs.rowWise(sc, data, nfs, scoreClassName, clVector))
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
