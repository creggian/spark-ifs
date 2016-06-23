/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package creggian.mrmr.feature.common

import java.lang.Math

class Score extends AbstractScore {

	def computeMI(matrix: Array[Array[Long]]): Double = {
		
		val tot = matrix.transpose.map(_.sum).sum
		val colSum = matrix.transpose.map(_.sum)

		var mi = 0.0
		for (j <- 0 until matrix.size) {
			val x = matrix(j) // row
			val rowSum = x.sum
			for (i <- 0 until x.size) {
				val e = x(i) // element
				
				val pxy = e.toDouble / tot
				val px = rowSum.toDouble / tot
				val py = colSum(i).toDouble / tot

				if (pxy > 0.0) {
                                	mi = mi + (pxy * (Math.log( pxy/(px*py) ) / Math.log(2)))
                                	//mi = mi + (pxy * (Math.log( pxy/(px*py) )))
                        	}
			}
		}

		mi
	}

	/*
	 * var s = new Score()
	 * val a = Array(1,2,2,1,1).map(_.toDouble)
	 * val b = Array(1,1,1,1,2).map(_.toDouble)
	 * s.mutualInformationDiscrete(a,b,Array(1.0,2.0),Array(1.0,2.0)) // Double = 0.17095059445466854
	 * s.mutualInformationDiscrete(b,a,Array(1.0,2.0),Array(1.0,2.0)) // Double = 0.17095059445466854
	 */
	def mutualInformationDiscrete(a: Array[Double], b: Array[Double], aLevels: Array[Double], bLevels: Array[Double]): Double = {

		// MI with the class
		var mat = Array.fill[Long](bLevels.size,aLevels.size)(0)
		for (i <- 0 until b.size) {
			val aValue = a(i) // real value
			val bValue = b(i) // real value

			val aValueIdxMat = aLevels.indexWhere(_ == aValue)
			val bValueIdxMat = bLevels.indexWhere(_ == bValue)

			mat(bValueIdxMat)(aValueIdxMat) = mat(bValueIdxMat)(aValueIdxMat) + 1
		}
		this.computeMI(mat)
	}

	def mrmrMutualInformation(variableArray: Array[Double], classArray: Array[Double], selectedVariablesArray: Array[Array[Double]], variableLevels: Array[Double], classLevels: Array[Double]): Double = {

		val v = variableArray
		val clV = classArray
		val sVariable = selectedVariablesArray
		val fl = variableLevels
		val cl = classLevels

		var mrmrClass = this.mutualInformationDiscrete(v, clV, fl, cl)

		// MI with the features
		var mrmrFeatures = 0.0
		for (j <- 0 until sVariable.size) {
			val sv = sVariable(j)
			mrmrFeatures = mrmrFeatures + this.mutualInformationDiscrete(v, sv, fl, fl)
		}

		var coefficient = 1.0
		if (sVariable.size > 1) {
			coefficient = 1.0 / (sVariable.size).toDouble
		}

		mrmrClass - (coefficient * mrmrFeatures)
	}

	def getResult(variableArray: Array[Double], classArray: Array[Double], selectedVariablesArray: Array[Array[Double]], variableLevels: Array[Double], classLevels: Array[Double]): Double = {
		this.mrmrMutualInformation(variableArray, classArray, selectedVariablesArray, variableLevels, classLevels)
	}
}	
