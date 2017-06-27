package com.datatorrent.lib.metrics;

import java.io.IOException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;

/**
 * This operator writes incoming tuples to files.
 * MetaData about the files is emitted on the output port for downstream processing (if any) and
 * also consists of number of tuples appended into the file.
 *
 * @param <INPUT>
 *          Type for incoming tuples. Converter needs to be defined which
 *          converts these tuples to byte[]. Default converters for String,
 *          byte[] tuples are provided in S3TupleOutputModule.
 *
 * @since 3.7.0
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class FSRecordCompactionOperator<INPUT> extends GenericFileOutputOperator<INPUT>
{
  /**
   * Output port for emitting metadata for finalized files.
   */
  public transient DefaultOutputPort<org.apache.apex.malhar.lib.fs.FSRecordCompactionOperator.OutputMetaData> output = new DefaultOutputPort<org.apache.apex.malhar.lib.fs.FSRecordCompactionOperator.OutputMetaData>();

  /**
   * Queue for holding finalized files for emitting on output port
   */
  private Queue<OutputMetaData> emitQueue = new LinkedBlockingQueue<OutputMetaData>();

  @NotNull
  String outputDirectoryName = "COMPACTION_OUTPUT_DIR";

  @NotNull
  String outputFileNamePrefix = "tuples-";

  private long tupleCount;
  private Map<String, Long> fileToTupleCount;

  public FSRecordCompactionOperator()
  {
    filePath = "";
    outputFileName = outputFileNamePrefix;
    maxLength = 128 * 1024 * 1024L;
    tupleCount = 0;
    fileToTupleCount = new ConcurrentHashMap<String, Long>();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    filePath = context.getValue(DAG.APPLICATION_PATH) + Path.SEPARATOR + outputDirectoryName;
    outputFileName = outputFileNamePrefix + context.getValue(DAG.APPLICATION_ID);
    super.setup(context);
  }

  @Override
  protected void processTuple(INPUT tuple)
  {
    super.processTuple(tuple);
    tupleCount++;
  }

  @Override
  protected void rotate(String fileName) throws IllegalArgumentException, IOException, ExecutionException
  {
    MutableInt mi = openPart.get(fileName);
    String partFileName = getPartFileName(fileName, mi.getValue());
    fileToTupleCount.put(partFileName, tupleCount);
    tupleCount = 0;
    super.rotate(fileName);
  }

  @Override
  protected void finalizeFile(String fileName) throws IOException
  {
    super.finalizeFile(fileName);

    String src = filePath + Path.SEPARATOR + fileName;
    Path srcPath = new Path(src);
    long offset = fs.getFileStatus(srcPath).getLen();

    //Add finalized files to the queue
    OutputMetaData metaData = new OutputMetaData(src, fileName, offset, fileToTupleCount.remove(fileName));
    //finalizeFile is called from committed callback.
    //Tuples should be emitted only between beginWindow to endWindow. Thus using emitQueue.
    emitQueue.add(metaData);
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    //Emit finalized files from the queue
    while (!emitQueue.isEmpty()) {
      output.emit(emitQueue.poll());
    }
  }

  public String getOutputDirectoryName()
  {
    return outputDirectoryName;
  }

  public void setOutputDirectoryName(@NotNull String outputDirectoryName)
  {
    this.outputDirectoryName = Preconditions.checkNotNull(outputDirectoryName);
  }

  public String getOutputFileNamePrefix()
  {
    return outputFileNamePrefix;
  }

  public void setOutputFileNamePrefix(@NotNull String outputFileNamePrefix)
  {
    this.outputFileNamePrefix = Preconditions.checkNotNull(outputFileNamePrefix);
  }

  /**
   * Metadata for output file for downstream processing
   */
  public static class OutputMetaData extends org.apache.apex.malhar.lib.fs.FSRecordCompactionOperator.OutputMetaData
  {
    private long noOfTuplesWritten;

    public OutputMetaData()
    {
    }

    public OutputMetaData(String path, String fileName, long size, long noOfTuplesWritten)
    {
      super(path, fileName, size);
      this.noOfTuplesWritten = noOfTuplesWritten;
    }

    public long getNoOfTuplesWritten()
    {
      return noOfTuplesWritten;
    }

    public void setNoOfTuplesWritten(long noOfTuplesWritten)
    {
      this.noOfTuplesWritten = noOfTuplesWritten;
    }
  }

}
