package com.datatorrent.testbench;

import java.util.ArrayList;
import java.util.List;

import org.apache.apex.api.operator.ControlTuple;

import com.datatorrent.api.ControlTupleEnabledSink;

/**
 * A control tuple enabled sink implementation to collect expected test results.
 * <p>
 * @displayName Collector Test Sink
 * @category Test Bench
 * @tags sink
 */
public class CollectorTestSink<T> implements ControlTupleEnabledSink<T>
{
  public final List<T> collectedTuples = new ArrayList<T>();
  public final List<ControlTuple> collectedControlTuples = new ArrayList<>();
  /**
   * clears data
   */
  public void clear()
  {
    this.collectedTuples.clear();
    this.collectedControlTuples.clear();
  }

  @Override
  public void put(T payload)
  {
    synchronized (collectedTuples) {
      collectedTuples.add(payload);
      collectedTuples.notifyAll();
    }
  }

  public void waitForResultCount(int count, long timeoutMillis) throws InterruptedException
  {
    while (collectedTuples.size() < count && timeoutMillis > 0) {
      timeoutMillis -= 20;
      synchronized (collectedTuples) {
        if (collectedTuples.size() < count) {
          collectedTuples.wait(20);
        }
      }
    }
  }

  public void waitForResultCount(int tupleCount, int controlTupleCount, long timeoutMillis) throws InterruptedException
  {
    while (collectedTuples.size() < tupleCount && collectedControlTuples.size() < controlTupleCount && timeoutMillis > 0) {
      timeoutMillis -= 20;
      synchronized (collectedTuples) {
        if (collectedTuples.size() < tupleCount) {
          collectedTuples.wait(20);
        }
      }
      timeoutMillis -= 20;
      synchronized (collectedControlTuples) {
        if (collectedControlTuples.size() < controlTupleCount) {
          collectedControlTuples.wait(20);
        }
      }
    }
  }

  @Override
  public int getCount(boolean reset)
  {
    synchronized (collectedTuples) {
      try {
        return collectedTuples.size();
      } finally {
        if (reset) {
          collectedTuples.clear();
          collectedControlTuples.clear();
        }
      }
    }
  }

  @Override
  public boolean putControl(ControlTuple controlTuple)
  {
    collectedControlTuples.add(controlTuple);
    return false;
  }
}
