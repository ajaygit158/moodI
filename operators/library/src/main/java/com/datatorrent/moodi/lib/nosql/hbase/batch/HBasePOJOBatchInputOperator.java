package com.datatorrent.moodi.lib.nosql.hbase.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.api.ControlAwareDefaultOutputPort;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.batch.BatchControlTuple;
import com.datatorrent.batch.Batchable;
import com.datatorrent.contrib.hbase.HBasePOJOInputOperator;

public class HBasePOJOBatchInputOperator extends HBasePOJOInputOperator
    implements Operator.CheckpointNotificationListener, Batchable
{

  protected boolean startBatchEmitted = false;
  protected boolean endBatchEmitted = false;
  private long shutdownWindowId = -1;
  private transient long currentWindowId;

  public final transient ControlAwareDefaultOutputPort<Object> outputPort = new ControlAwareDefaultOutputPort<Object>()
  {
    @Override
    public void setup(com.datatorrent.api.Context.PortContext context)
    {
      pojoType = context.getAttributes().get(Context.PortContext.TUPLE_CLASS);
    }
  };

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    currentWindowId = windowId;
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
  protected void emitTuple(Object tuple)
  {
    getOutputPort().emit(tuple);
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

  protected boolean shouldEmitStartBatch()
  {
    return !startBatchEmitted;
  }

  protected boolean shouldEmitEndBatch()
  {
    //If no tuples were emitted in the emitTuples call
    return (!endBatchEmitted && tuplesRead == 0);
  }

  protected DefaultOutputPort<Object> getOutputPort()
  {
    return this.outputPort;
  }

  private static final Logger LOG = LoggerFactory.getLogger(HBasePOJOBatchInputOperator.class);

  @Override
  public void checkpointed(long windowId)
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

  @Override
  public void beforeCheckpoint(long windowId)
  {
  }
}
