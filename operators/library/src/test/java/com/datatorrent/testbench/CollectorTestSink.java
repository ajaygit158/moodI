/**
* LIMITED LICENSE
* THE TERMS OF THIS LIMITED LICENSE (?AGREEMENT?) GOVERN YOUR USE OF THE SOFTWARE, DOCUMENTATION AND ANY OTHER MATERIALS MADE
* AVAILABLE ON THIS SITE (?LICENSED MATERIALS?) BY DATATORRENT.  ANY USE OF THE LICENSED MATERIALS IS GOVERNED BY THE FOLLOWING
* TERMS AND CONDITIONS.  IF YOU DO NOT AGREE TO THE FOLLOWING TERMS AND CONDITIONS, YOU DO NOT HAVE THE RIGHT TO DOWNLOAD OR
* VIEW THE LICENSED MATERIALS.  

* Under this Agreement, DataTorrent grants to you a personal, limited, non-exclusive, non-assignable, non-transferable
*  non-sublicenseable, revocable right solely to internally view and evaluate the Licensed Materials. DataTorrent reserves
*  all rights not expressly granted in this Agreement. 
* Under this Agreement, you are not granted the right to install or operate the Licensed Materials. To obtain a license
* granting you a license with rights beyond those granted under this Agreement, please contact DataTorrent at www.datatorrent.com. 
* You do not have the right to, and will not, reverse engineer, combine, modify, adapt, copy, create derivative works of,
* sublicense, transfer, distribute, perform or display (publicly or otherwise) or exploit the Licensed Materials for any purpose
* in any manner whatsoever.
* You do not have the right to, and will not, use the Licensed Materials to create any products or services which are competitive
* with the products or services of DataTorrent.
* The Licensed Materials are provided to you 'as is' without any warranties. DATATORRENT DISCLAIMS ANY AND ALL WARRANTIES, EXPRESS
* OR IMPLIED, INCLUDING THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, TITLE AND NON-INFRINGEMENT AND ANY
* WARRANTIES ARISING FROM A COURSE OR PERFORMANCE, COURSE OF DEALING OR USAGE OF TRADE.  DATATORRENT AND ITS LICENSORS SHALL NOT
* BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF
* OR IN CONNECTION WITH THE LICENSED MATERIALS.
 */
package com.datatorrent.testbench;

import java.util.ArrayList;
import java.util.List;

import org.apache.apex.api.operator.ControlTuple;

import com.datatorrent.api.ControlTupleEnabledSink;

/**
 * A control tuple enabled sink implementation to collect expected test results.
 * <p>
 * 
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
    while (collectedTuples.size() < tupleCount && collectedControlTuples.size() < controlTupleCount
        && timeoutMillis > 0) {
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
