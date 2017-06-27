package com.datatorrent.lib.metrics;

import java.io.IOException;

import org.apache.apex.malhar.lib.fs.FSRecordCompactionOperator;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;

/**
 * S3Reconciler extends from org.apache.apex.malhar.lib.fs.s3.S3Reconciler.
 * It emits the bytesWrittenPerSecond, bytesWritten, eventsWrittenPerSecond, eventsWritten metrics.
 */
public class S3Reconciler extends org.apache.apex.malhar.lib.fs.s3.S3Reconciler
{
  @AutoMetric
  private long bytesWrittenPerSecond;
  @AutoMetric
  private long totalBytesWritten = 0;
  @AutoMetric
  private long eventsWrittenPerSecond;
  @AutoMetric
  private long totalEventsWritten = 0;
  private long eventsWrittenPerWindow;
  private long bytesWrittenPerWindow;
  private double windowTimeSec;

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    windowTimeSec = (context.getValue(Context.OperatorContext.APPLICATION_WINDOW_COUNT) *
      context.getValue(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS) * 1.0) / 1000.0;
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    bytesWrittenPerWindow = 0;
    bytesWrittenPerSecond = 0;
    eventsWrittenPerWindow = 0;
    eventsWrittenPerSecond = 0;
  }

  @Override
  protected void removeIntermediateFiles(FSRecordCompactionOperator.OutputMetaData metaData)
  {
    Path path = new Path(metaData.getPath());
    try {
      if (!fs.exists(path)) {
        return;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    bytesWrittenPerWindow += metaData.getSize();
    if (metaData instanceof com.datatorrent.lib.metrics.FSRecordCompactionOperator.OutputMetaData) {
      eventsWrittenPerWindow += ((com.datatorrent.lib.metrics.FSRecordCompactionOperator.OutputMetaData)metaData).getNoOfTuplesWritten();
    }
    super.removeIntermediateFiles(metaData);
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    bytesWrittenPerSecond = (long)(bytesWrittenPerWindow/windowTimeSec);
    eventsWrittenPerSecond = (long)(eventsWrittenPerWindow/windowTimeSec);
    totalBytesWritten += bytesWrittenPerWindow;
    totalEventsWritten += eventsWrittenPerWindow;
  }
}
