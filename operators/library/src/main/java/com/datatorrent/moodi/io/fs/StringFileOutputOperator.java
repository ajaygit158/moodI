package com.datatorrent.moodi.io.fs;

import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator;

import com.datatorrent.api.AutoMetric;

public class StringFileOutputOperator extends GenericFileOutputOperator.StringFileOutputOperator
{
  @AutoMetric
  private long totalBytes;

  @Override
  protected byte[] getBytesForTuple(String tuple)
  {
    byte[] result = super.getBytesForTuple(tuple);
    totalBytes += result.length;
    return result;
  }
}
