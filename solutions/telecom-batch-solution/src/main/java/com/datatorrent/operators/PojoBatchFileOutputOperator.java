package com.datatorrent.operators;

import com.datatorrent.io.fs.AbstractBatchFileOutputOperator;

public class PojoBatchFileOutputOperator extends AbstractBatchFileOutputOperator<Object>
{
  private String outputFileName;

  protected String getFileName(Object tuple)
  {
    return outputFileName + "_" + this.context.getId();
  }

  protected byte[] getBytesForTuple(Object tuple)
  {
    return (new StringBuilder()).append(tuple.toString()).append("\n").toString().getBytes();
  }

  public String getOutputFileName()
  {
    return outputFileName;
  }

  public void setOutputFileName(String outputFileName)
  {
    this.outputFileName = outputFileName;
  }
}
