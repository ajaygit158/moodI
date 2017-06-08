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

package com.datatorrent.io.fs;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.wal.WindowDataManager;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.io.fs.AbstractBatchFileInputOperator.DirectoryScanner.ScannerState;
import com.datatorrent.lib.io.fs.AbstractFileInputOperator;

/**
 * This is the base implementation of a file input operator, which scans a
 * directory for files.&nbsp; Files are then read and split into tuples, which
 * are emitted.&nbsp; Subclasses should implement the methods required to read
 * and emit tuples from files.
 * <p>
 * Derived class defines how to read entries from the input stream and emit to
 * the port.
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
public abstract class AbstractBatchFileInputOperator<T> extends AbstractFileInputOperator<T>
{
  private static final Logger LOG = LoggerFactory.getLogger(AbstractBatchFileInputOperator.class);

  /**
   * By setting this property to true, a single scan will be performed every
   * batchIntervalMillis. All files made available in this scan period will form
   * a part of same batch. For each batch, start batch and end batch control
   * tuples will be emitted before and after the batch respectively. Note that
   * batch tuple processing currently works only in single operator instance
   * scenarios i.e without partitions. However, batch processing with parallel
   * partitions will work
   */
  private boolean emitBatchTuples = false;

  /**
   * Once shutdown condition is reached, we store the windowId in shutdown
   * window id. When this window gets committed, we can initiate shutdown
   */
  private long shutdownWindowId;
  private boolean performShutdown = false;

  private boolean deferEmitTuplesToNextWindow = false;

  @NotNull
  private WindowDataManager windowControlDataManager = new WindowDataManager.NoopWindowDataManager();

  protected final transient WindowRecoveryEntry currentWindowBatchState = new WindowRecoveryEntry();

  protected static class WindowRecoveryEntry
  {
    boolean startBatchEmitted;
    boolean endBatchEmitted;
    boolean endApplicationEmitted;
    ScannerState scannerState;

    public WindowRecoveryEntry()
    {
      startBatchEmitted = false;
      endBatchEmitted = false;
      endApplicationEmitted = false;
      scannerState = null;
    }

    public WindowRecoveryEntry(boolean startBatchEmitted, boolean endBatchEmitted, boolean endApplicationEmitted,
        ScannerState scannerState)
    {
      this.startBatchEmitted = startBatchEmitted;
      this.endBatchEmitted = endBatchEmitted;
      this.endApplicationEmitted = endApplicationEmitted;
      this.scannerState = scannerState;
    }

    public void clear()
    {
      startBatchEmitted = false;
      endBatchEmitted = false;
      endApplicationEmitted = false;
      scannerState = null;
    }
  }

  public AbstractBatchFileInputOperator()
  {
    DirectoryScanner batchScanner = new DirectoryScanner();
    this.scanner = batchScanner;
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    if (scanner instanceof DirectoryScanner) {
      DirectoryScanner batchScanner = (DirectoryScanner)scanner;
      batchScanner.setBatchMode(emitBatchTuples);
      batchScanner.setEndApplication(performShutdown);
    }
    windowControlDataManager.setup(context);
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    deferEmitTuplesToNextWindow = false;
  }

  @Override
  public void emitTuples()
  {
    if (deferEmitTuplesToNextWindow) {
      return;
    }
    super.emitTuples();
  }

  @Override
  public void endWindow()
  {
    if (currentWindowId > windowControlDataManager.getLargestCompletedWindow()) {
      //save the state of scanner so that after recovery, the scanner is at correct state
      if (scanner instanceof DirectoryScanner) {
        currentWindowBatchState.scannerState = ((DirectoryScanner)scanner).getScannerState();
      }
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
        }
        if (recoveryData.endApplicationEmitted) {
          emitEndApplicationControlTuple();
        }
        if (scanner instanceof DirectoryScanner) {
          DirectoryScanner batchScanner = (DirectoryScanner)scanner;
          batchScanner.setScannerState(recoveryData.scannerState);
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
    if (performShutdown && windowId > shutdownWindowId) {
      LOG.info("Requesting shutdown");
      throw new ShutdownException();
    }
  }

  @Override
  protected void scanDirectory()
  {
    //additional guard
    if (scanner instanceof DirectoryScanner) {
      DirectoryScanner batchScanner = (DirectoryScanner)scanner;
      ScannerState scannerState = batchScanner.updateState();
      if (scannerState == ScannerState.EMIT_START_BATCH) {
        LOG.debug("Emitting start batch control tuple");
        emitStartBatchControlTuple();
        //start batch is immediate. We can perform scan in same window
        batchScanner.informStartBatchWasEmitted();
        currentWindowBatchState.startBatchEmitted = true;
      }
      if (scannerState == ScannerState.EMIT_END_BATCH) {
        LOG.debug("Emitting end batch control tuple");
        emitEndBatchControlTuple();
        //start batch is immediate. We can perform scan in same window
        batchScanner.informEndBatchWasEmitted();
        currentWindowBatchState.endBatchEmitted = true;
        deferEmitTuplesToNextWindow = true;
      }
      if (scannerState == ScannerState.EMIT_END_APPLICATION) {
        LOG.debug("Emitting end application control tuple");
        emitEndApplicationControlTuple();
        currentWindowBatchState.endApplicationEmitted = true;
        //We will throw shutdown exception in a window after shutdownWindowId
        shutdownWindowId = currentWindowId;
        deferEmitTuplesToNextWindow = true;
      }
      if (scannerState == ScannerState.SCAN) {
        super.scanDirectory();
      }
    } else {
      super.scanDirectory();
    }
  }

  public abstract void emitStartBatchControlTuple();

  public abstract void emitEndBatchControlTuple();

  public abstract void emitEndApplicationControlTuple();

  /**
   * The class that is used to scan for new files in the directory for the
   * AbstractBatchFileInputOperator. By default, if batch is enabled, it will
   * perform a single scan of the input directory and emit a single batch only
   */
  @SuppressWarnings("serial")
  @Evolving
  public static abstract class DirectoryScanner extends com.datatorrent.lib.io.fs.AbstractFileInputOperator.DirectoryScanner
  {
    private boolean batchMode;
    private boolean endApplication;
    private ScannerState scannerState = ScannerState.START;
    private boolean allFilesScanned = false;
    private long scanCount = 0;
    private static final long DEFAULT_MAX_SCAN_COUNT = 1;
    private long maxScanCount = DEFAULT_MAX_SCAN_COUNT;

    @Override
    public LinkedHashSet<Path> scan(FileSystem fs, Path filePath, Set<String> consumedFiles)
    {
      LinkedHashSet<Path> fileList = super.scan(fs, filePath, consumedFiles);
      scanCount++;
      if (shouldEndScan()) {
        allFilesScanned = true;
      }
      return fileList;
    }

    public enum ScannerState
    {
      START, EMIT_START_BATCH, START_BATCH_EMITTED, SCAN, EMIT_END_BATCH, END_BATCH_EMITTED, EMIT_END_APPLICATION, FINISH
    }

    public ScannerState updateState()
    {
      LOG.debug("Scanner state before update : {}", scannerState);
      switch (scannerState) {
        case START:
          if (batchMode) {
            scannerState = ScannerState.EMIT_START_BATCH;
          } else {
            scannerState = ScannerState.SCAN;
          }
          break;
        case EMIT_START_BATCH:
          //wait till scannerState is updated to startBatchEmitted
          break;
        case START_BATCH_EMITTED:
          //we can start scanning for files
          scannerState = ScannerState.SCAN;
          break;
        case SCAN:
          if (allFilesScanned) {
            if (batchMode) {
              scannerState = ScannerState.EMIT_END_BATCH;
            } else if (endApplication) {
              scannerState = ScannerState.EMIT_END_APPLICATION;
            }
          } else {
            scannerState = ScannerState.SCAN;
          }
          break;
        case EMIT_END_BATCH:
          //the user of scanner needs to update the state by calling endBatchEmitted()
          break;
        case END_BATCH_EMITTED:
          if (shouldStartNewBatch()) {
            scannerState = ScannerState.EMIT_START_BATCH;
          } else {
            scannerState = ScannerState.EMIT_END_APPLICATION;
          }
          break;
        case EMIT_END_APPLICATION:
          scannerState = ScannerState.FINISH;
          break;
        default:
          break;
      }
      LOG.debug("Scanner state after update : {}", scannerState);
      return scannerState;
    }

    protected boolean shouldEndScan()
    {
      //By default we will end the scan after first
      if (scanCount >= 1) {
        return true;
      }
      return false;
    }

    protected boolean shouldStartNewBatch()
    {
      //false by default, only one batch will be emitted
      return false;
    }

    public void informStartBatchWasEmitted()
    {
      scannerState = ScannerState.START_BATCH_EMITTED;
    }

    public void informEndBatchWasEmitted()
    {
      scannerState = ScannerState.END_BATCH_EMITTED;
    }

    public boolean isBatchMode()
    {
      return batchMode;
    }

    public void setBatchMode(boolean batchMode)
    {
      this.batchMode = batchMode;
    }

    public boolean isEndApplication()
    {
      return endApplication;
    }

    public void setEndApplication(boolean endApplication)
    {
      this.endApplication = endApplication;
    }

    public void setMaxScanCount(long maxScanCount)
    {
      this.maxScanCount = maxScanCount;
    }

    public long getMaxScanCount()
    {
      return maxScanCount;
    }

    public ScannerState getScannerState()
    {
      return scannerState;
    }

    protected void setScannerState(ScannerState scannerState)
    {
      this.scannerState = scannerState;
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

  public void setEmitBatchTuples(boolean emitBatchTuples)
  {
    this.emitBatchTuples = emitBatchTuples;
  }

  public boolean isEmitBatchTuples()
  {
    return emitBatchTuples;
  }

  public void setPerformShutdown(boolean performShutdown)
  {
    this.performShutdown = performShutdown;
  }

  public boolean isPerformShutdown()
  {
    return performShutdown;
  }
}
