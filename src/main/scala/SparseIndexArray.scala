package creggian.collection.mutable

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Exception

class SparseIndexArray(size: Long, dense: Boolean = true) extends Serializable {
    
    var added: ArrayBuffer[Long] = new ArrayBuffer[Long]()
    var removed: ArrayBuffer[Long] = new ArrayBuffer[Long]()
    
    def add(idx: Long) {
        if (!dense && !added.contains(idx)) added += idx
        if (dense && removed.contains(idx)) removed -= idx
    }
    
    def remove(idx: Long): Unit = {
        if (!dense && added.contains(idx)) added -= idx
        if (dense && !removed.contains(idx)) removed += idx
    }
    
    def contains(idx: Long): Boolean = {
        var ret = false
        if (dense && idx < getSize && !removed.contains(idx)) ret = true
        if (!dense && idx < getSize && added.contains(idx)) ret = true
        ret
    }
    
    def getAdded: ArrayBuffer[Long] = {
        if (!dense) added
        else throw new java.lang.RuntimeException("!!! SparseIndexArray getAdded in sparse context not possible !!!")
    }
    
    def getSize: Long = {
        size
    }
}
