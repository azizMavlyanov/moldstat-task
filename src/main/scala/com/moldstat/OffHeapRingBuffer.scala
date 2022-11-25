package com.moldstat

import java.lang.foreign.{MemoryAddress, MemorySegment, MemorySession, ValueLayout}


trait OffHeapRingBuffer {
  def capacity: Int // total amount of messages

  def messageLength: Int // fixed message length

  def offset(msg: MemorySegment): Boolean // return true if capacity is not reached

  def poll(): MemorySegment // return null if buffer is empty
}

class OffHeapRingBufferImpl(override val capacity: Int, override val messageLength: Int) extends OffHeapRingBuffer {
  val globalStorageSize: Long = capacity * messageLength
  lazy val globalMemorySegment: MemorySegment = MemorySegment.allocateNative(globalStorageSize, MemorySession.openImplicit())
  @volatile var currentSetOffset: Long = -1
  @volatile var currentGetOffset: Long = 0
  @volatile var currentStorageSize: Long = globalStorageSize

  def isFull(msg: MemorySegment): Boolean = ((currentSetOffset - currentGetOffset) + 1 == capacity) || currentStorageSize < msg.byteSize()

  def isEmpty: Boolean = currentSetOffset < currentGetOffset;

  override def offset(msg: MemorySegment): Boolean = {
    if (currentSetOffset > capacity || currentStorageSize < msg.byteSize()) {
      false
    } else {
      currentSetOffset = currentSetOffset + 1
      globalMemorySegment.setAtIndex(ValueLayout.ADDRESS, currentSetOffset, msg.address())
      currentStorageSize = currentStorageSize - msg.byteSize()
      true
    }
  }

  override def poll(): MemorySegment = {
    if (isEmpty) {
      null
    }
    else {
      val memoryAddress: MemoryAddress = globalMemorySegment.getAtIndex(ValueLayout.ADDRESS, currentGetOffset)
      currentGetOffset = currentGetOffset + 1
      val memoryByteSize = globalStorageSize - currentStorageSize
      val memorySegment = MemorySegment.ofAddress(memoryAddress, memoryByteSize, MemorySession.openImplicit())
      memorySegment
    }
  }
}

object OffHeapRingBufferImpl {
  def apply(capacity: Int, messageLength: Int): OffHeapRingBufferImpl = {
    new OffHeapRingBufferImpl(capacity, messageLength)
  }
}
