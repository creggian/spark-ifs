package creggian.mrmr.lib

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.storage.StorageLevel

import scala.reflect.runtime._
import scala.tools.reflect.ToolBox

import creggian.collection.mutable.SparseIndexArray
import creggian.mrmr.feature.common.FeatureWiseAbstractScore
import creggian.mrmr.feature.common.InstanceWiseAbstractScore

class IterativeFeatureSelection {
    
    def columnWise(sc: SparkContext, data: RDD[(LabeledPoint)], nfs: Int, scoreClassName: String): Array[(String, Double)] = {
    
        val classLevels    = data.map(x => x.label).distinct().collect()
        val featuresLevels = data.map({
            case (LabeledPoint(_, v: SparseVector)) => v.values.distinct
            case (LabeledPoint(_, v: DenseVector))  => v.values.distinct
        }).reduce((x, y) => (x ++ y).distinct)
        
        val classLevels_bc    = sc.broadcast(classLevels)
        val featuresLevels_bc = sc.broadcast(featuresLevels)
    
        // scala reflection
        val tb = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()
        val score = tb.eval(tb.parse("new " + scoreClassName + "()")).asInstanceOf[InstanceWiseAbstractScore]
        
        val nCols = data.first.features.size
        
        val candidateIdx = new SparseIndexArray(nCols, dense=true)
        val selectedIdx = new SparseIndexArray(nCols, dense=false)
        var selectedIdxScore = Array[Double]()
        
        for (_ <- 1 to nfs) {
            
            val candidateIdx_bc = sc.broadcast(candidateIdx)
            val selectedIdx_bc = sc.broadcast(selectedIdx)
            
            val contingencyTables = data.mapPartitions(iter => {
                val caIdx = candidateIdx_bc.value
                val seIdx = selectedIdx_bc.value
                val cl = classLevels_bc.value
                val fl = featuresLevels_bc.value
                val cln = cl.length
                val fln = fl.length
                
                val matWithClassMap    = scala.collection.mutable.Map[(Long, Long), Array[Array[Long]]]()
                val matWithFeaturesMap = scala.collection.mutable.Map[(Long, Long), Array[Array[Long]]]()
                
                while (iter.hasNext) {
                    val lp = iter.next
                    val label = lp.label
                    val features = lp.features
                    
                    var fcIdx = 0L
                    while (fcIdx < caIdx.getSize) {
                        if (caIdx.contains(fcIdx)) {
                            val value = features(fcIdx.toInt)
                            val matWithClassMapKey = (fcIdx, -1L)  // -1L represent the class idx, it is used as placeholder
    
                            val classValueIdxMat = cl.indexWhere(_ == label)
                            val valueIdxMat = fl.indexWhere(_ == value)
    
                            val matWithClass = if (matWithClassMap.contains(matWithClassMapKey)) matWithClassMap(matWithClassMapKey) else Array.fill[Long](cln, fln)(0)
    
                            matWithClass(classValueIdxMat)(valueIdxMat) = matWithClass(classValueIdxMat)(valueIdxMat) + 1
                            matWithClassMap(matWithClassMapKey) = matWithClass
    
                            for (fsIdx <- seIdx.getAdded) {
                                val valueSelected = features(fsIdx.toInt)
                                val matWithFeaturesMapKey = (fcIdx, fsIdx)
        
                                val valueSelectedIdxMat = fl.indexWhere(_ == valueSelected)
                                val valueIdxMat = fl.indexWhere(_ == value)
        
                                val matWithFeatures = if (matWithFeaturesMap.contains(matWithFeaturesMapKey)) matWithFeaturesMap(matWithFeaturesMapKey) else Array.fill[Long](fln, fln)(0)
        
                                matWithFeatures(valueSelectedIdxMat)(valueIdxMat) = matWithFeatures(valueSelectedIdxMat)(valueIdxMat) + 1
                                matWithFeaturesMap(matWithFeaturesMapKey) = matWithFeatures
                            }
                        }
                        fcIdx += 1
                    }
                }
                
                (matWithClassMap.toList ++ matWithFeaturesMap.toList).iterator
            })
            
            val contingencyTablesReduce = contingencyTables.reduceByKey((x, y) => x.zip(y).map(x => x._1.zip(x._2).map(y => y._1 + y._2)))
            
            val ct = contingencyTablesReduce.map(x => (x._1._1, (x._1._2, x._2))) // ((idx, clIdx|idxSelected), contingencyTable) => (idx, (clIdx|idxSelected, contingencyTable))
    
            val step2 = ct.groupByKey.map(entry => {
                val (key, buffer) = entry
                
                val seIdx = selectedIdx_bc.value
                val cl = classLevels_bc.value
                val fl = featuresLevels_bc.value
                
                var matWithClass = Array[Array[Long]]()
                var matWithFeatures = Array[Array[Array[Long]]]()
                
                val iter = buffer.iterator
                while (iter.hasNext) {
                    val tuple = iter.next
                    val (idx, mat) = tuple
                    
                    if (idx == -1L) matWithClass = mat
                    else matWithFeatures = matWithFeatures ++ Array(mat)
                }
    
                val candidateScore = score.getResult(matWithClass, matWithFeatures, seIdx.getAdded.toArray, fl, cl)
                
                (key, candidateScore)
            })
            
            
            val step3 = step2.takeOrdered(1)(Ordering[Double].reverse.on(_._2)) //.foreach(println)
            
            // Example of step3 output (key, score)
            //   (2818,0.8462824341015066)
            
            selectedIdx.add(step3(0)._1)
            selectedIdxScore = selectedIdxScore ++ Array(step3(0)._2)
            candidateIdx.remove(step3(0)._1)
        }
    
        selectedIdx.getAdded.toArray.map(_.toString).zip(selectedIdxScore)
    }
    
    def rowWise(sc: SparkContext, data: RDD[(String, Array[Double])], nfs: Int, scoreClassName: String, classVector: Array[Double]): Array[(String, Double)] = {
        val rdd = data.sortByKey(numPartitions = 500).persist(StorageLevel.MEMORY_AND_DISK_SER)
        
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
        
        for (i <- 1 to nfs) {
            
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
