package creggian.examples.ifs

import org.apache.spark._
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import creggian.ml.feature.IterativeFeatureSelection

object CommandLine {
    def main(args: Array[String]) {
        run(args)
    }
    
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
            
            val data = sc.textFile(filename).map(x => {
                val arr = x.split(separator)
                val label = arr(labelIdx).toDouble
                val v = arr.take(labelIdx) ++ arr.drop(labelIdx + 1)
                val dv = Vectors.dense(v.map(_.toDouble))
                new LabeledPoint(label, dv.compressed)
            })
            
            val rdd = data.repartition(numPartitions = nPartition).persist(StorageLevel.MEMORY_AND_DISK_SER)
            val clVector = if (typeWise == "row-wise") Vectors.dense(sc.textFile(args(6)).first().split(separator).map(_.toDouble)) else null
            
            start(sc, rdd, typeWise, nfs, scoreClassName, clVector)
        }
        
        if (inputFormat == "libsvm") {
            val data = MLUtils.loadLibSVMFile(sc, filename).map(lp => new LabeledPoint(lp.label, lp.features.compressed))
            
            val rdd = data.repartition(numPartitions = nPartition).persist(StorageLevel.MEMORY_AND_DISK_SER)
            val clVector = if (typeWise == "row-wise") Vectors.dense(sc.textFile(args(6)).first().split(",").map(_.toDouble)) else null
    
            start(sc, rdd, typeWise, nfs, scoreClassName, clVector)
        }
    }
    
    def start(sc: SparkContext, data: RDD[(LabeledPoint)], typeWise: String, nfs: Int, scoreClassName: String, clVector: Vector) {
        printStats(data)
        
        val ifs = new IterativeFeatureSelection()
        
        if (typeWise == "column-wise") this.printResults(ifs.columnWise(sc, data, nfs, scoreClassName))
        if (typeWise == "row-wise")    this.printResults(ifs.rowWise(sc, data, nfs, scoreClassName, clVector))
    }
    
    def printStats(data: RDD[(LabeledPoint)]): Unit = {
        val nRows: Long = data.count()
        val nCols: Int = data.first.features.size
        val nData: Int = data.map(x => x.features.numNonzeros + 1).reduce(_ + _)
        
        println()
        println("Input dataset stats:")
        println("-- number or rows:            " + nRows)
        println("-- number or columns:         " + nCols)
        println("-- number or non-zero points: " + nData)
    }
    
    def printResults(res: Array[(Long, Double)]) {
        println("\nSelected Features:")
        println("Order\tName\tScore")
        for (i <- res.indices) {
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

