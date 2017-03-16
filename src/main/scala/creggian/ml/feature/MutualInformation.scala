package creggian.ml.feature

class MutualInformation {
    
    def computeMI(matrix: Array[Array[Long]]): Double = {
        
        val tot = matrix.transpose.map(_.sum).sum
        val colSum = matrix.transpose.map(_.sum)
        
        var mi = 0.0
        for (j <- matrix.indices) {
            val x = matrix(j)
            // row
            val rowSum = x.sum
            for (i <- x.indices) {
                val e = x(i) // element
                
                val pxy = e.toDouble / tot
                val px = rowSum.toDouble / tot
                val py = colSum(i).toDouble / tot
                
                if (pxy > 0.0) {
                    mi = mi + (pxy * (Math.log(pxy / (px * py)) / Math.log(2)))
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
        val mat = Array.fill[Long](bLevels.length, aLevels.length)(0)
        for (i <- b.indices) {
            val aValue = a(i)
            // real value
            val bValue = b(i) // real value
            
            val aValueIdxMat = aLevels.indexWhere(_ == aValue)
            val bValueIdxMat = bLevels.indexWhere(_ == bValue)
            
            mat(bValueIdxMat)(aValueIdxMat) = mat(bValueIdxMat)(aValueIdxMat) + 1
        }
        this.computeMI(mat)
    }
    
}
