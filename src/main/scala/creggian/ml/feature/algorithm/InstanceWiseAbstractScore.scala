package creggian.ml.feature.algorithm

abstract class InstanceWiseAbstractScore extends Serializable {
    
    def getResult(matWithClass: Array[Array[Long]],
                  matWithFeatures: Array[Array[Array[Long]]],
                  selectedVariablesArray: Array[Long],
                  variableLevels: Array[Double],
                  classLevels: Array[Double],
                  i: Int,
                  nfs: Int): Double
    
    def selectTop(i: Int, nfs: Int): Int = {
        1
    }
    
    def maxIterations(nfs: Int): Int = {
        nfs
    }
    
}
