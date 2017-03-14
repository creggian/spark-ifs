package creggian.mrmr.feature.common

abstract class InstanceWiseAbstractScore extends Serializable {
    
    def getResult(matWithClass: Array[Array[Long]],
                  matWithFeatures: Array[Array[Array[Long]]],
                  selectedVariablesArray: Array[Long],
                  variableLevels: Array[Double],
                  classLevels: Array[Double]): Double
    
}
