package com.datatorrent.io.fs;

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.api.ControlAwareDefaultInputPort;
import org.apache.apex.api.operator.ControlTuple;

import com.datatorrent.api.StreamCodec;
import com.datatorrent.batch.BatchControlTuple;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

public abstract class AbstractBatchFileOutputOperator<INPUT> extends AbstractFileOutputOperator<INPUT>
{
  private static final Logger LOG = LoggerFactory.getLogger(AbstractBatchFileOutputOperator.class);

  public final transient ControlAwareDefaultInputPort<INPUT> input = new ControlAwareDefaultInputPort<INPUT>()
  {
    @Override
    public void process(INPUT tuple)
    {
      processTuple(tuple);
    }

    @Override
    public StreamCodec<INPUT> getStreamCodec()
    {
      if (AbstractBatchFileOutputOperator.this.streamCodec == null) {
        return super.getStreamCodec();
      } else {
        return streamCodec;
      }
    }

    @Override
    public boolean processControl(ControlTuple controlTuple)
    {
      processControlTuple(controlTuple);
      //This is an output operator. Does not matter what value we return.
      return false;
    }
  };

  public void processControlTuple(ControlTuple controlTuple)
  {
    if (controlTuple instanceof BatchControlTuple.EndBatchControlTuple) {
      for (Map.Entry<String, String> entry : this.getFileNameToTmpName().entrySet()) {
        try {
          LOG.debug("Finalizing file : {}", entry.getKey());
          finalizeFile(entry.getKey());
        } catch (IOException e) {
          LOG.error("Failed to finalize file {}", entry.getKey());
          throw new RuntimeException(e);
        }
      }
    }
  }
}
