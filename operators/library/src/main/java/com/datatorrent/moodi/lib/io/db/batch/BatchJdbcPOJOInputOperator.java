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
package com.datatorrent.moodi.lib.io.db.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.api.ControlAwareDefaultOutputPort;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.batch.BatchControlTuple;
import com.datatorrent.batch.Batchable;

/**
 * A concrete input operator that reads from a database through the JDBC
 * API.<br/>
 * This operator by default uses the fieldInfos and the table name to construct
 * the sql query.<br/>
 * The operator emits start batch before reading any tuples from the database.
 * Once all records are read and there are no more records available, the
 * operator will emit end batch control tuple. The application is shutdown after
 * the shutdown window Id is committed by the DAG.
 */
@Evolving
public class BatchJdbcPOJOInputOperator extends JdbcPOJOInputOperator
    implements Operator.CheckpointNotificationListener, Batchable
{

  protected boolean startBatchEmitted = false;
  protected boolean endBatchEmitted = false;
  protected transient long recordsEmittedTillPreviousWindow;
  private long shutdownWindowId = -1;
  private transient long currentWindowId;

  public final transient ControlAwareDefaultOutputPort<Object> outputPort = new ControlAwareDefaultOutputPort<Object>()
  {
    @Override
    public void setup(Context.PortContext context)
    {
      pojoClass = context.getValue(Context.PortContext.TUPLE_CLASS);
    }
  };

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    currentWindowId = windowId;
    recordsEmittedTillPreviousWindow = tuplesRead;
  }

  @Override
  public void emitTuples()
  {
    if (shouldEmitStartBatch()) {
      emitStartBatchControlTuple();
      startBatchEmitted = true;
    }
    super.emitTuples();
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    if (shouldEmitEndBatch()) {
      emitEndBatchControlTuple();
      endBatchEmitted = true;
      shutdownWindowId = this.currentWindowId;
    }
  }

  @Override
  public void emitStartBatchControlTuple()
  {
    if (getOutputPort() instanceof ControlAwareDefaultOutputPort) {
      BatchControlTuple startBatchControlTuple = new BatchControlTuple.StartBatchControlTupleImpl();
      ControlAwareDefaultOutputPort<Object> output = (ControlAwareDefaultOutputPort<Object>)getOutputPort();
      output.emitControl(startBatchControlTuple);
    } else {
      LOG.error("Output port is not control aware, skipped emitting start batch control tuple");
    }
  }

  @Override
  public void emitEndBatchControlTuple()
  {
    if (getOutputPort() instanceof ControlAwareDefaultOutputPort) {
      BatchControlTuple endBatchControlTuple = new BatchControlTuple.EndBatchControlTupleImpl();
      ControlAwareDefaultOutputPort<Object> output = (ControlAwareDefaultOutputPort<Object>)getOutputPort();
      output.emitControl(endBatchControlTuple);
    } else {
      LOG.error("Output port is not control aware, skipped emitting end batch control tuple");
    }
  }

  /**
   * decides and returns whether start batch control tuple should be emitted or
   * not
   *
   * @return
   */
  protected boolean shouldEmitStartBatch()
  {
    return !startBatchEmitted;
  }

  /**
   * decides and returns whether end batch control tuple should be emitted or
   * not
   *
   * @return
   */
  protected boolean shouldEmitEndBatch()
  {
    //If no tuples were emitted in the emitTuples call
    return (!endBatchEmitted && recordsEmittedTillPreviousWindow == tuplesRead);
  }

  protected DefaultOutputPort<Object> getOutputPort()
  {
    return this.outputPort;
  }

  @Override
  public void checkpointed(long windowId)
  {
  }

  @Override
  public void beforeCheckpoint(long windowId)
  {
  }

  @Override
  public void committed(long windowId)
  {
    //Shutdown the application after all records are read.
    if (shutdownWindowId != -1 && windowId > shutdownWindowId) {
      throw new ShutdownException();
    }
  }

  public static final Logger LOG = LoggerFactory.getLogger(BatchJdbcPOJOInputOperator.class);
}
