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
package creggian.ml.feature.algorithm

import creggian.ml.feature.MutualInformation
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.mllib.regression.LabeledPoint

import scala.collection.immutable.Range

class FeatureWiseMRMR extends FeatureWiseAbstractScore {
    
    private def mutualInformationDiscrete(a: Vector, b: Vector, aLevels: Array[Double], bLevels: Array[Double]): Double = {
    
        if (a.size != b.size) throw new RuntimeException("Vectors 'a' and 'b' must have the same length: a = " + a.size + " - b = " + b.size)
        
        val mat = Array.fill[Long](bLevels.length, aLevels.length)(0L)
        
        val aIdx = a match {
            case v: SparseVector => v.indices.toIndexedSeq
            case v: DenseVector  => Range(0, v.size)
        }
        val bIdx = b match {
            case v: SparseVector => v.indices.toIndexedSeq
            case v: DenseVector  => Range(0, v.size)
        }
        
        var counter = 0L
        for (i <- aIdx) {
            val aValue = a(i) // real value
            val bValue = if (bIdx.contains(i)) b(i) else 0.0// real value
            
            val aValueIdxMat = aLevels.indexWhere(_ == aValue)
            val bValueIdxMat = bLevels.indexWhere(_ == bValue)
            
            mat(bValueIdxMat)(aValueIdxMat) = mat(bValueIdxMat)(aValueIdxMat) + 1L
            counter += 1L
        }
    
        for (i <- bIdx) {
            if (!aIdx.contains(i)) {
                val aValue = 0.0 // real value
                val bValue = b(i) // real value
    
                val aValueIdxMat = aLevels.indexWhere(_ == aValue)
                val bValueIdxMat = bLevels.indexWhere(_ == bValue)
    
                mat(bValueIdxMat)(aValueIdxMat) = mat(bValueIdxMat)(aValueIdxMat) + 1L
                counter += 1L
            }
        }
    
        val aZeroIdxMat = aLevels.indexWhere(_ == 0.0)
        val bZeroIdxMat = bLevels.indexWhere(_ == 0.0)
        if (aZeroIdxMat >= 0 && bZeroIdxMat >= 0) mat(bZeroIdxMat)(aZeroIdxMat) = a.size - counter
    
        val mi = new MutualInformation()
        mi.computeMI(mat)
    }
    
    private def mrmrMutualInformation(featureVector: Vector, classVector: Vector, selectedVariablesArray: Array[LabeledPoint]): Double = {
        
        val classLevels = classVector match {
            case v: SparseVector => v.values.distinct ++ Array(0.0)
            case v: DenseVector  => v.values.distinct
        }
        val variableLevelsAll = for (vector <- Array(featureVector) ++ selectedVariablesArray.map(x => x.features)) yield {
            vector match {
                case v: SparseVector => v.values.distinct ++ Array(0.0)
                case v: DenseVector  => v.values.distinct
            }
        }
        val variableLevels = variableLevelsAll.reduce(_ ++ _).distinct
    
        val mrmrClass = mutualInformationDiscrete(featureVector, classVector, variableLevels, classLevels)
        
        var mrmrFeatures = 0.0
        for (j <- selectedVariablesArray.indices) {
            val sLP = selectedVariablesArray(j)
            mrmrFeatures = mrmrFeatures + mutualInformationDiscrete(featureVector, sLP.features, variableLevels, variableLevels)
        }
            
        var coefficient = 1.0
        if (selectedVariablesArray.length > 1) coefficient = 1.0 / selectedVariablesArray.length.toDouble
    
        mrmrClass - (coefficient * mrmrFeatures)
    }
    
    def getResult(featureVector: Vector, classVector: Vector, selectedVariablesArray: Array[LabeledPoint], i: Int, nfs: Int): Double = {
        this.mrmrMutualInformation(featureVector, classVector, selectedVariablesArray)
    }
    
    override def selectTop(i: Int, nfs: Int): Int = {
        1
    }
}	
