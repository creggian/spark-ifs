package creggian.examples.ifs

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils

import creggian.ml.feature.IterativeFeatureSelection

object IFSExample {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("IFS Example")
        val sc        = new SparkContext(sparkConf)
        
        val data = MLUtils.loadLibSVMFile(sc, "data/mrmr.50x20.cw.c0.x1_8.libsvm")
        val rdd  = data.map(lp => new LabeledPoint(lp.label, lp.features.compressed))
        val nfs  = 10
        val scn  = "creggian.ml.feature.algorithm.InstanceWiseMRMR"
    
        val ifs     = new IterativeFeatureSelection()
        val results = ifs.columnWise(sc, rdd, nfs, scn)
        val rddRed  = ifs.compress(rdd, results.map(_._1))
    
        println(rddRed.first().toString())
    }
}
