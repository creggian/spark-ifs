package creggian.mrmr.lib

import org.apache.spark._
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._
import scala.reflect.runtime._
import scala.tools.reflect.ToolBox

import creggian.mrmr.feature.common.FeatureWiseAbstractScore
import creggian.mrmr.feature.common.InstanceWiseAbstractScore

class IterativeFeatureSelection {
    
    def columnWise(sc: SparkContext, rdd: RDD[(String, Array[Double])], nfs: Int, scoreClassName: String): Array[(String, Double)] = {
    
        val classLevels     = rdd.map(x => x._1.toDouble).distinct().collect()
        val featuresLevels  = rdd.map(x => x._2.distinct).reduce((x, y) => (x ++ y).distinct)
        
        val classLevels_bc    = sc.broadcast(classLevels)
        val featuresLevels_bc = sc.broadcast(featuresLevels)
    
        // scala reflection
        val tb = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()
        val score = tb.eval(tb.parse("new " + scoreClassName + "()")).asInstanceOf[InstanceWiseAbstractScore]
        
        val nCols = rdd.map(_._2).take(1)(0).length
        
        var candidateIdx = (0 until nCols).toArray
        var selectedIdx = Array[Int]()
        var selectedIdxScore = Array[Double]()
        
        for (i <- 1 to nfs) {
            
            val candidateIdx_bc = sc.broadcast(candidateIdx)
            val selectedIdx_bc = sc.broadcast(selectedIdx)
            
            
            val step1Class = rdd.mapPartitions(iter => {
                val caIdx = candidateIdx_bc.value
                val seIdx = selectedIdx_bc.value
                val cl = classLevels_bc.value
                val fl = featuresLevels_bc.value
                val cln = cl.size
                val fln = fl.size
                
                var matWithClassMap = scala.collection.mutable.Map[(Int, Int), Array[Array[Long]]]()
                
                while (iter.hasNext) {
                    val arr = iter.next
                    val classValue = arr._1.toDouble
                    val arrCandidate = caIdx.map(x => arr._2(x))
                    val arrSelected = seIdx.map(x => arr._2(x))
                    
                    // to optimize see http://stackoverflow.com/questions/9137644/how-to-get-the-element-index-when-mapping-an-array-in-scala
                    
                    val arrCandidateWithIndex = arrCandidate.zip(caIdx)
                    for (j <- 0 until arrCandidateWithIndex.size) {
                        val tuple = arrCandidateWithIndex(j)
                        val (value, idx) = tuple
                        val matWithClassMapKey = (idx, -1)  // -1 represent the class idx, it is used as placeholder
                        
                        val classValueIdxMat = cl.indexWhere(_ == classValue)
                        val valueIdxMat = fl.indexWhere(_ == value)
                        
                        var matWithClass = Array.fill[Long](cln, fln)(0)
                        if (matWithClassMap.contains(matWithClassMapKey)) {
                            matWithClass = matWithClassMap(matWithClassMapKey)
                        }
                        
                        matWithClass(classValueIdxMat)(valueIdxMat) = matWithClass(classValueIdxMat)(valueIdxMat) + 1
                        matWithClassMap(matWithClassMapKey) = matWithClass
                    }
                }
                
                matWithClassMap.toList.iterator
            })
            
            
            val step1Features = rdd.mapPartitions(iter => {
                val caIdx = candidateIdx_bc.value
                val seIdx = selectedIdx_bc.value
                val cl = classLevels_bc.value
                val fl = featuresLevels_bc.value
                val fln = fl.size
                
                var matWithFeaturesMap = scala.collection.mutable.Map[(Int, Int), Array[Array[Long]]]()
                
                while (iter.hasNext) {
                    val arr = iter.next
                    val arrCandidate = caIdx.map(x => arr._2(x))
                    val arrSelected = seIdx.map(x => arr._2(x))
                    
                    // to optimize see http://stackoverflow.com/questions/9137644/how-to-get-the-element-index-when-mapping-an-array-in-scala
                    
                    val arrCandidateWithIndex = arrCandidate.zip(caIdx)
                    val arrSelectedWithIndex = arrSelected.zip(seIdx)
                    for (j <- 0 until arrCandidateWithIndex.size) {
                        val tuple = arrCandidateWithIndex(j)
                        val (value, idx) = tuple
                        for (k <- 0 until arrSelectedWithIndex.size) {
                            val tupleSelected = arrSelectedWithIndex(k)
                            val (valueSelected, idxSelected) = tupleSelected
                            val matWithFeaturesMapKey = (idx, idxSelected)
                            
                            val valueSelectedIdxMat = fl.indexWhere(_ == valueSelected)
                            val valueIdxMat = fl.indexWhere(_ == value)
                            
                            var matWithFeatures = Array.fill[Long](fln, fln)(0)
                            if (matWithFeaturesMap.contains(matWithFeaturesMapKey)) {
                                matWithFeatures = matWithFeaturesMap(matWithFeaturesMapKey)
                            }
                            
                            matWithFeatures(valueSelectedIdxMat)(valueIdxMat) = matWithFeatures(valueSelectedIdxMat)(valueIdxMat) + 1
                            matWithFeaturesMap(matWithFeaturesMapKey) = matWithFeatures
                        }
                    }
                }
                
                matWithFeaturesMap.toList.iterator
            })
            
            
            val step1ClassReduce    = step1Class   .reduceByKey((x, y) => x.zip(y).map(x => x._1.zip(x._2).map(y => y._1 + y._2)))
            val step1FeaturesReduce = step1Features.reduceByKey((x, y) => x.zip(y).map(x => x._1.zip(x._2).map(y => y._1 + y._2)))
            
            val step1ClassReduceMap = step1ClassReduce.map(x => {
                // ((idx, clIdx), matWithClass) = x
                val idx = x._1._1
                val clIdx = x._1._2
                val matWithClass = x._2
                
                (idx, (clIdx, matWithClass))
            })
            
            val step1FeaturesReduceMap = step1FeaturesReduce.map(x => {
                // ((idx, idxSelected), matWithFeatures) = x
                val idx = x._1._1
                val idxSelected = x._1._2
                val matWithFeatures = x._2
                
                (idx, (idxSelected, matWithFeatures))
            })
            
            val step2 = step1ClassReduceMap.union(step1FeaturesReduceMap).groupByKey.map(entry => {
                val (key, buffer) = entry
                
                val seIdx = selectedIdx_bc.value
                val cl = classLevels_bc.value
                val fl = featuresLevels_bc.value
                
                var matWithClass = Array[Array[Long]]()
                var matWithFeatures = Array[Array[Array[Long]]]()
                
                val iter = buffer.iterator
                while (iter.hasNext()) {
                    val tuple = iter.next
                    val (idx, mat) = tuple
                    
                    if (idx == -1) {
                        matWithClass = mat
                    } else {
                        matWithFeatures = matWithFeatures ++ Array(mat)
                    }
                }
    
                val mrmr_i = score.getResult(matWithClass, matWithFeatures, seIdx, fl, cl)
                
                (key, mrmr_i)
            })
            
            
            val step3 = step2.takeOrdered(1)(Ordering[Double].reverse.on(_._2)) //.foreach(println)
            
            // Example of step3 output (key, score)
            //   (2818,0.8462824341015066)
            
            selectedIdx = selectedIdx ++ Array(step3(0)._1)
            selectedIdxScore = selectedIdxScore ++ Array(step3(0)._2)
            candidateIdx = candidateIdx.diff(Array(step3(0)._1))
        }
        
        selectedIdx.map(_.toString).zip(selectedIdxScore)
    }
    
    def rowWise(sc: SparkContext, rdd: RDD[(String, Array[Double])], nfs: Int, scoreClassName: String, classVector: Array[Double]): Array[(String, Double)] = {
        val classLevels     = classVector.distinct
        val featuresLevels  = rdd.map(x => x._2.distinct).reduce((x, y) => (x ++ y).distinct)
        
        val classLevels_bc = sc.broadcast(classLevels)
        val featuresLevels_bc = sc.broadcast(featuresLevels)
        val clVector_bc = sc.broadcast(classVector)
        
        var selectedVariableName = Array[String]()
        var selectedVariableScore = Array[Double]()
        var selectedVariable = Array[Array[Double]]()
        
        // scala reflection
        val tb = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()
        val score = tb.eval(tb.parse("new " + scoreClassName + "()")).asInstanceOf[FeatureWiseAbstractScore]
        
        for (ii <- 1 to nfs) {
            
            val selectedVariableName_bc = sc.broadcast(selectedVariableName)
            val selectedVariable_bc = sc.broadcast(selectedVariable)
            
            val varrCandidate = rdd.filter(x => !selectedVariableName_bc.value.contains(x._1)).persist
            
            // in varrCandidate.map, 'x' is a string variable, it still has to be mapped
            // into an array of Int, because the 'headerIdx' of the vector is the
            // column name
            val step2 = varrCandidate.map(s => {
                val clV = clVector_bc.value
                val cl = classLevels_bc.value
                val fl = featuresLevels_bc.value
                val sVariable = selectedVariable_bc.value
                
                val variableLabel = s._1
                val v = s._2
                var mrmr_i = score.getResult(v, clV, sVariable, fl, cl)
                
                (variableLabel, mrmr_i)
            })
            
            val step3 = step2.takeOrdered(1)(Ordering[Double].reverse.on(_._2)) //.foreach(println)
            
            val newSelectedLabel = step3(0)._1
            val newSelectedLabel_bc = sc.broadcast(newSelectedLabel)
            val newSelectedArr = varrCandidate.filter(s => s._1 == newSelectedLabel_bc.value).flatMap(s => s._2).collect
            
            selectedVariableName = selectedVariableName ++ Array(newSelectedLabel)
            selectedVariableScore = selectedVariableScore ++ Array(step3(0)._2)
            selectedVariable = selectedVariable ++ Array(newSelectedArr)
        }
        
        selectedVariableName.zip(selectedVariableScore)
    }
}
