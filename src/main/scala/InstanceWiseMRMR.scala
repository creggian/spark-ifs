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

class InstanceWiseMRMR extends InstanceWiseAbstractScore {
    
    private def mrmrMutualInformation(matWithClass: Array[Array[Long]], matWithFeatures: Array[Array[Array[Long]]], selectedVariablesIdx: Array[Long]): Double = {
        val mi = new MutualInformation()
        
        val mrmrClass = mi.computeMI(matWithClass)
    
        var mrmrFeatures = 0.0
        for (f <- matWithFeatures) {
            mrmrFeatures = mrmrFeatures + mi.computeMI(f)
        }
    
        var coefficient = 1.0
        if (selectedVariablesIdx.size > 1) {
            coefficient = 1.0 / selectedVariablesIdx.size.toDouble
        }
        mrmrClass - (coefficient * mrmrFeatures)
    }
    
    def getResult(matWithClass: Array[Array[Long]], matWithFeatures: Array[Array[Array[Long]]], selectedVariablesIdx: Array[Long], variableLevels: Array[Double], classLevels: Array[Double]): Double = {
        this.mrmrMutualInformation(matWithClass, matWithFeatures, selectedVariablesIdx)
    }
}
