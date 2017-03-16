package creggian.ml.feature.algorithm

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint

abstract class FeatureWiseAbstractScore extends Serializable {
    
    def getResult(featureVector: Vector,
                  classVector: Vector,
                  selectedVariablesArray: Array[LabeledPoint],
                  i: Int,
                  nfs: Int): Double
    
    def selectTop(i: Int, nfs: Int): Int = {
        1
    }
    
    def maxIterations(nfs: Int): Int = {
        nfs
    }
    
}	
