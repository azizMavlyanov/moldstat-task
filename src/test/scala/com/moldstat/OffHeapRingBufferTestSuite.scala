package com.moldstat

import java.lang.foreign.{SegmentAllocator, ValueLayout}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

class OffHeapRingBufferTestSuite extends munit.FunSuite {
  val segmentAllocator: FunFixture[SegmentAllocator] = FunFixture[SegmentAllocator] (
    setup = { _ =>
      SegmentAllocator.implicitAllocator();
    },
    teardown = { _ => }
  )

  test("should return set capacity and messageLength") {
    val expectedCapacity = 24
    val expectedMessageLength = 4
    val offHeapRingBuffer = OffHeapRingBufferImpl(24, 4)
    val obtainedCapacity = offHeapRingBuffer.capacity
    val obtainedMessageLength = offHeapRingBuffer.messageLength

    assertEquals(obtainedCapacity, expectedCapacity)
    assertEquals(obtainedMessageLength, expectedMessageLength)
  }

  segmentAllocator.test("should return true while offsetting") { allocator =>
    val chars = Array('a', 'b', 'c')
    val memorySegment = allocator.allocateArray(ValueLayout.ADDRESS, chars.length);
    for (i <- chars.indices) {
      memorySegment.setAtIndex(ValueLayout.JAVA_CHAR, i, chars(i))
    }
    val ringBuffer: OffHeapRingBufferImpl = OffHeapRingBufferImpl(12, 3)
    val obtained = ringBuffer.offset(memorySegment)
    val expected = true
    assertEquals(obtained, expected)
  }

  segmentAllocator.test("should return correct memory segment while polling") { allocator =>
    val chars = Array('a', 'b', 'c')
    val memorySegment = allocator.allocateArray(ValueLayout.ADDRESS, chars.length);
    for (i <- chars.indices) {
      memorySegment.setAtIndex(ValueLayout.JAVA_CHAR, i, chars(i))
    }
    val ringBuffer: OffHeapRingBufferImpl = OffHeapRingBufferImpl(12, 3)
    ringBuffer.offset(memorySegment)
    val obtained = ringBuffer.poll().address()
    val expected = memorySegment.address()
    assertEquals(obtained, expected)
  }

  segmentAllocator.test("should return null while polling") { allocator =>
    val ringBuffer: OffHeapRingBufferImpl = OffHeapRingBufferImpl(12, 3)
    val obtained = ringBuffer.poll()
    val expected = null
    assertEquals(obtained, expected)
  }

  segmentAllocator.test("should return false while offsetting") { allocator =>
    val chars = Array('a', 'b', 'c')
    val memorySegment = allocator.allocateArray(ValueLayout.ADDRESS, chars.length);
    for (i <- chars.indices) {
      memorySegment.setAtIndex(ValueLayout.JAVA_CHAR, i, chars(i))
    }
    val ringBuffer: OffHeapRingBufferImpl = OffHeapRingBufferImpl(3, 1)
    val obtained = ringBuffer.offset(memorySegment)
    val expected = false
    assertEquals(obtained, expected)
  }

  segmentAllocator.test("should check offset value while multi thread processing") { allocator =>
    val chars = Array('a', 'b', 'c')
    val memorySegment = allocator.allocateArray(ValueLayout.ADDRESS, chars.length);
    for (i <- chars.indices) {
      memorySegment.setAtIndex(ValueLayout.JAVA_CHAR, i, chars(i))
    }
    val ringBuffer: OffHeapRingBufferImpl = OffHeapRingBufferImpl(48, 3)

    val numberOfThreads = 5
    val latch = new CountDownLatch(numberOfThreads)

    for (_ <- 0 to numberOfThreads) {
      Future {
        try ringBuffer.offset(memorySegment)
        catch {
          case e: InterruptedException =>
          // Handle exception
        }
        latch.countDown()
      }
    }

    latch.await()
    assertEquals(numberOfThreads, ringBuffer.currentSetOffset.toInt)
  }

}
