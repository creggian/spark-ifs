package creggian.ml.feature

import creggian.collection.mutable.SparseIndexArray
import creggian.ml.feature.algorithm.{FeatureWiseAbstractScore, InstanceWiseAbstractScore}
import org.apache.spark._
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import scala.collection.immutable.Map
import scala.collection.mutable.{Map => MMap}
import scala.reflect.runtime._
import scala.tools.reflect.ToolBox

class IterativeFeatureSelection {
    
    def columnWise(sc: SparkContext, data: RDD[(LabeledPoint)], nfs: Int, scoreClassName: String): Array[(Long, Double)] = {
        
        val classValuesTable:    Map[Double, Long]             = data.map(x => (x.label, 1L)).reduceByKey(_ + _).collect().toMap
        val selectedValuesTable: MMap[Long, Map[Double, Long]] = MMap[Long, Map[Double, Long]]()
        
        val classLevels:            Array[Double]     = classValuesTable.keys.toArray
        val featuresLevelsNotZero : Array[Double]     = data.map({
            case (LabeledPoint(_, v: SparseVector)) => v.values.distinct
            case (LabeledPoint(_, v: DenseVector))  => v.values.distinct
        }).reduce((x, y) => (x ++ y).distinct)
        
        val featuresLevels = (featuresLevelsNotZero ++ Array(0.0)).distinct
        
        val classValuesTable_bc = sc.broadcast(classValuesTable)
        val classLevels_bc      = sc.broadcast(classLevels)
        val featuresLevels_bc   = sc.broadcast(featuresLevels)
        
        // scala reflection
        val tb    = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()
        val score = tb.eval(tb.parse("new " + scoreClassName + "()")).asInstanceOf[InstanceWiseAbstractScore]
        
        val nCols: Int  = data.first.features.size
        
        val candidateIdx     = new SparseIndexArray(nCols, dense=true)
        val selectedIdx      = new SparseIndexArray(nCols, dense=false)
        var selectedIdxScore = Array[Double]()
        
        for (i <- 1 to score.maxIterations(nfs)) {
            
            val candidateIdx_bc        = sc.broadcast(candidateIdx)
            val selectedIdx_bc         = sc.broadcast(selectedIdx)
            val selectedValuesTable_bc = sc.broadcast(selectedValuesTable)
            
            val contingencyTableItems = data.mapPartitions(iter => {
                val caIdx = candidateIdx_bc.value
                val seIdx = selectedIdx_bc.value
                val cl    = classLevels_bc.value
                val fl    = featuresLevels_bc.value
    
                // ((candidateFeatureIdx, otherFeatureIdx, candidateIdxCT, otherFeatureIdxCT), count)
                val matWithClassMap    = MMap[(Long, Long, Int, Int), Long]()
                val matWithFeaturesMap = MMap[(Long, Long, Int, Int), Long]()
                
                while (iter.hasNext) {
                    val lp = iter.next
                    val label = lp.label
                    val features = lp.features
                    
                    features match {
                        case v: SparseVector =>
                            for (fcIdx <- v.indices) {
                                if (caIdx.contains(fcIdx)) {
                                    val value            = features(fcIdx.toInt)
                                    val classValueIdxMat = cl.indexWhere(_ == label)
                                    val valueIdxMat      = fl.indexWhere(_ == value)
        
                                    val matWithClassMapKey = (fcIdx.toLong, -1L, valueIdxMat, classValueIdxMat)  // -1L represent the class idx, it is used as placeholder
        
                                    val matWithClass = if (matWithClassMap.contains(matWithClassMapKey)) matWithClassMap(matWithClassMapKey) else 0L
                                    matWithClassMap(matWithClassMapKey) = matWithClass + 1L
        
                                    for (fsIdx <- seIdx.getAdded) {
                                        val valueSelected       = features(fsIdx.toInt)
                                        val valueSelectedIdxMat = fl.indexWhere(_ == valueSelected)
                                        val valueIdxMat         = fl.indexWhere(_ == value)
            
                                        val matWithFeaturesMapKey = (fcIdx.toLong, fsIdx, valueIdxMat, valueSelectedIdxMat)
            
                                        val matWithFeatures = if (matWithFeaturesMap.contains(matWithFeaturesMapKey)) matWithFeaturesMap(matWithFeaturesMapKey) else 0L
                                        matWithFeaturesMap(matWithFeaturesMapKey) = matWithFeatures + 1L
                                    }
                                }
                            }
                        case v: DenseVector =>
                            for (fcIdx <- 0L until v.size) {
                                if (caIdx.contains(fcIdx) && features(fcIdx.toInt) != 0.0) {
                                    val value            = features(fcIdx.toInt)
                                    val classValueIdxMat = cl.indexWhere(_ == label)
                                    val valueIdxMat      = fl.indexWhere(_ == value)
    
                                    val matWithClassMapKey = (fcIdx.toLong, -1L, valueIdxMat, classValueIdxMat)  // -1L represent the class idx, it is used as placeholder
    
                                    val matWithClass = if (matWithClassMap.contains(matWithClassMapKey)) matWithClassMap(matWithClassMapKey) else 0L
                                    matWithClassMap(matWithClassMapKey) = matWithClass + 1L
    
                                    for (fsIdx <- seIdx.getAdded) {
                                        val valueSelected       = features(fsIdx.toInt)
                                        val valueSelectedIdxMat = fl.indexWhere(_ == valueSelected)
                                        val valueIdxMat         = fl.indexWhere(_ == value)
        
                                        val matWithFeaturesMapKey = (fcIdx.toLong, fsIdx, valueIdxMat, valueSelectedIdxMat)
        
                                        val matWithFeatures = if (matWithFeaturesMap.contains(matWithFeaturesMapKey)) matWithFeaturesMap(matWithFeaturesMapKey) else 0L
                                        matWithFeaturesMap(matWithFeaturesMapKey) = matWithFeatures + 1L
                                    }
                                }
                            }
                        case other =>
                            throw new UnsupportedOperationException(
                                s"Only sparse and dense vectors are supported but got ${other.getClass}.")
                    }
                }
                
                (matWithClassMap.toList ++ matWithFeaturesMap.toList).iterator
            })
    
            val contingencyTableItemsReduce = contingencyTableItems.reduceByKey(_ + _)
            
            val contingencyTablesKV = contingencyTableItemsReduce.map(x => (x._1._1, (x._1._2, x._1._3, x._1._4, x._2)))
            
            val candidateScores = contingencyTablesKV.groupByKey.map(entry => {
                val (key, buffer) = entry
                
                val cvt = classValuesTable_bc.value
                val fvt = selectedValuesTable_bc.value
                val cl = classLevels_bc.value
                val fl = featuresLevels_bc.value
                val cln = cl.length
                val fln = fl.length
    
                // create the intermediate data structures for discrete features
                val matWithClass       = Array.fill[Long](cln, fln)(0)
                val matWithFeaturesMap = scala.collection.mutable.Map[Long, Array[Array[Long]]]()
                
                // fill in the data from the Map step into the intermediate data structures
                val iter = buffer.iterator
                while (iter.hasNext) {
                    val item = iter.next
                    val (otherIdx, fcMatIdx, otherMatIdx, value) = item
    
                    if (otherIdx == -1L) {
                        matWithClass(otherMatIdx)(fcMatIdx) = value
                    } else {
                        if (matWithFeaturesMap.contains(otherIdx)) matWithFeaturesMap(otherIdx)(otherMatIdx)(fcMatIdx) = value
                        else {
                            matWithFeaturesMap(otherIdx) = Array.fill[Long](fln, fln)(0)
                            matWithFeaturesMap(otherIdx)(otherMatIdx)(fcMatIdx) = value
                        }
                    }
                }
                
                // to save bandwidth, we did not produced tuples where the candidate
                // feature's value id zero. This is particularly useful in sparse datasets
                // but it can save also some space in dense datasets.
                val fcIdxCtZero = fl.indexWhere(_ == 0.0)
                val marginalSum = matWithClass.map(_.sum)
                for (i <- 0 until cln) {
                    matWithClass(i)(fcIdxCtZero) = cvt(cl(i)) - marginalSum(i)
                }
                for (key <- matWithFeaturesMap.keys) {
                    val ct = matWithFeaturesMap(key)
                    val marginalSum = ct.map(_.sum)
                    for (i <- 0 until fln) {
                        matWithFeaturesMap(key)(i)(fcIdxCtZero) = fvt(key)(fl(i)) - marginalSum(i)
                    }
                }
    
                // create the missing data structured needed for the interface score class
                val selectedFeaturesIdx = matWithFeaturesMap.keys.toArray
                val matWithFeatures     = matWithFeaturesMap.values.toArray
                
                val candidateScore = score.getResult(matWithClass, matWithFeatures, selectedFeaturesIdx, fl, cl, i, nfs)
                
                (key, candidateScore)
            })
    
            val results = candidateScores.takeOrdered(score.selectTop(i, nfs))(Ordering[Double].reverse.on(_._2))
            
            for (ri <- results.indices) {
                // Example of step3 output (key, score) = (2818,0.8462824341015066)
                val novelSelectedFeatureIdx   = results(ri)._1
                val novelSelectedFeatureScore = results(ri)._2
    
                selectedIdx.add(novelSelectedFeatureIdx)
                selectedIdxScore = selectedIdxScore ++ Array(novelSelectedFeatureScore)
                selectedValuesTable(novelSelectedFeatureIdx) = data.map(x => (x.features(novelSelectedFeatureIdx.toInt), 1L)).reduceByKey(_ + _).collect().toMap
                candidateIdx.remove(novelSelectedFeatureIdx)
            }
        }
        
        selectedIdx.getAdded.toArray.map(_.toLong).zip(selectedIdxScore)
    }
    
    def rowWise(sc: SparkContext, data: RDD[LabeledPoint], nfs: Int, scoreClassName: String, classVector: Vector): Array[(Long, Double)] = {
        
        val clVector_bc = sc.broadcast(classVector)
        
        var selectedVariableScore = Array[Double]()
        var selectedVariable = Array[LabeledPoint]()
        
        // scala reflection
        val tb = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()
        val score = tb.eval(tb.parse("new " + scoreClassName + "()")).asInstanceOf[FeatureWiseAbstractScore]
        
        for (i <- 1 to score.maxIterations(nfs)) {
            
            val selectedVariable_bc = sc.broadcast(selectedVariable)
            val varrCandidate = data.filter(x => !selectedVariable_bc.value.map(lp => lp.label).contains(x.label))
            
            val candidateScores = varrCandidate.map(x => {
                val candidateScore = score.getResult(x.features, clVector_bc.value, selectedVariable_bc.value, i, nfs)
                (x.label, candidateScore)
            })
            
            val results = candidateScores.takeOrdered(score.selectTop(i, nfs))(Ordering[Double].reverse.on(_._2))
    
            for (ri <- results.indices) {
                val novelSelectedFeatureLabel = results(ri)._1
                val novelSelectedFeatureScore = results(ri)._2
    
                val newSelectedLabel_bc = sc.broadcast(novelSelectedFeatureLabel)
                val newSelectedLPAll = varrCandidate.filter(x => x.label == newSelectedLabel_bc.value)
    
                if (newSelectedLPAll.count() != 1L) throw new RuntimeException("Error: Multiple features with the same ID")
    
                val newSelectedLP = newSelectedLPAll.first()
    
                selectedVariableScore = selectedVariableScore ++ Array(novelSelectedFeatureScore)
                selectedVariable = selectedVariable ++ Array(newSelectedLP)
            }
        }
    
        selectedVariable.map(x => x.label.toLong).zip(selectedVariableScore)
    }
    
    def compress(rdd: RDD[LabeledPoint], selectedIdx: Array[Long]): RDD[LabeledPoint] = {
        rdd.map(x => {
            val label = x.label
            val features = Vectors.dense(selectedIdx.map(idx => x.features(idx.toInt)))
            new LabeledPoint(label, features.compressed)
        })
    }
}
