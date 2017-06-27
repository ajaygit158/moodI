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

package com.datatorrent.moodi.lib.io.fs.batch;

import java.io.IOException;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.wal.WindowDataManager;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.batch.Batchable;
import com.datatorrent.io.fs.utils.Scanner;
import com.datatorrent.lib.io.fs.AbstractFileInputOperator;

/**
 * This is the base implementation of a file input operator supporting batch
 * operations, which scans a directory for files.&nbsp; Before the records are
 * emitted, a start batch control tuple is emitted indicating start of batch
 * operation. Files are then read and split into tuples, which are
 * emitted.&nbsp; Subclasses should implement the methods required to read and
 * emit tuples from files. After all files are read, an end batch tuple is
 * emitted to indicate end of the batch. The operator then proceeds to shutdown
 * the operator.
 * <p>
 * Derived class defines how to read entries from the input stream, emit to the
 * port and emitting start and end batch control tuples.
 * </p>
 * <p>
 * The directory scanning logic is pluggable to support custom directory layouts
 * and naming schemes.If batch mode is enabled, the scanner will update its
 * state. The operator can read this state and emit start batch and end batch
 * tuples.
 * </p>
 * <p>
 * Partitioning and dynamic changes to number of partitions is not supported.
 * Partitioning in a batch application is possible only with parallel
 * partitioning.
 * </p>
 * 
 * @displayName File Input with Batch support
 * @category Input
 * @tags fs, file, input operator, batch
 *
 * @param <T>
 *          The type of the object that this input operator reads.
 */

@Evolving
public abstract class AbstractBatchFileInputOperator<T> extends AbstractFileInputOperator<T> implements Batchable
{
  private static final Logger LOG = LoggerFactory.getLogger(AbstractBatchFileInputOperator.class);

  /**
   * Once shutdown condition is reached, we store the windowId in shutdown
   * window id. When this window gets committed, we can initiate shutdown
   */
  private long shutdownWindowId = -1;
  private boolean isBatchStarted = false;
  private boolean isBatchCompleted = false;

  private boolean emitTuples = true;

  /**
   * Should be merged into windowControlManager in AbstractFileInputOperator
   * once the functionality is merged into malhar
   */
  @NotNull
  private WindowDataManager windowControlDataManager = new WindowDataManager.NoopWindowDataManager();

  protected final transient WindowRecoveryEntry currentWindowBatchState = new WindowRecoveryEntry();

  protected static class WindowRecoveryEntry
  {
    boolean startBatchEmitted;
    boolean endBatchEmitted;

    public WindowRecoveryEntry()
    {
      startBatchEmitted = false;
      endBatchEmitted = false;
    }

    public WindowRecoveryEntry(boolean startBatchEmitted, boolean endBatchEmitted)
    {
      this.startBatchEmitted = startBatchEmitted;
      this.endBatchEmitted = endBatchEmitted;
    }

    public void clear()
    {
      startBatchEmitted = false;
      endBatchEmitted = false;
    }
  }

  public AbstractBatchFileInputOperator()
  {
    //Initialize the default scanner to NoOpScanner.
    this.scanner = new Scanner.NoOpScanner();
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    windowControlDataManager.setup(context);
    if (scanner instanceof Scanner.NoOpScanner) {
      //Using Default scanner inplementation if no scanner provided
      scanner = new Scanner.SingleScanDirectoryScanner();
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
  }

  @Override
  public void emitTuples()
  {
    if (!emitTuples) {
      return;
    }
    super.emitTuples();
  }

  @Override
  public void endWindow()
  {
    if (currentWindowId > windowControlDataManager.getLargestCompletedWindow()) {
      try {
        windowControlDataManager.save(currentWindowBatchState, currentWindowId);
      } catch (IOException e) {
        throw new RuntimeException("saving recovery", e);
      }
    }
    currentWindowBatchState.clear();
    super.endWindow();
  }

  @Override
  protected void replay(long windowId)
  {
    try {
      Map<Integer, Object> recoveryDataPerOperator = windowControlDataManager.retrieveAllPartitions(windowId);
      for (Object recovery : recoveryDataPerOperator.values()) {
        WindowRecoveryEntry recoveryData = (WindowRecoveryEntry)recovery;
        if (recoveryData.startBatchEmitted) {
          emitStartBatchControlTuple();
        }
        super.replay(windowId);
        if (recoveryData.endBatchEmitted) {
          emitEndBatchControlTuple();
          emitTuples = false;
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("replay", e);
    }
  }

  @Override
  public void committed(long windowId)
  {
    try {
      windowControlDataManager.committed(windowId);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    super.committed(windowId);
    if (shutdownWindowId != -1 && windowId > shutdownWindowId) {
      LOG.info("Requesting shutdown");
      throw new ShutdownException();
    }
  }

  @Override
  protected void scanDirectory()
  {
    if (!isBatchStarted) {
      LOG.debug("Emitting start batch control tuple");
      emitStartBatchControlTuple();
      currentWindowBatchState.startBatchEmitted = true;
      isBatchStarted = true;
    }
    Scanner fileScanner = (Scanner)scanner;
    if (!fileScanner.isScanComplete()) {
      super.scanDirectory();
    } else if (!isBatchCompleted) {
      LOG.debug("Emitting end batch control tuple");
      emitEndBatchControlTuple();
      currentWindowBatchState.endBatchEmitted = true;
      isBatchCompleted = true;
      //End application in next window
      shutdownWindowId = currentWindowId;
      emitTuples = false;
      return;
    }
  }

  public WindowDataManager getWindowControlDataManager()
  {
    return windowControlDataManager;
  }

  public void setWindowControlDataManager(WindowDataManager windowControlDataManager)
  {
    this.windowControlDataManager = windowControlDataManager;
  }
}
