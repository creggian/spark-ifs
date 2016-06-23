package creggian.mrmr.feature.common

abstract class AbstractScore extends Serializable {

	def getResult(variableArray: Array[Double], classArray: Array[Double], selectedVariablesArray: Array[Array[Double]], variableLevels: Array[Double], classLevels: Array[Double]): Double

}	
