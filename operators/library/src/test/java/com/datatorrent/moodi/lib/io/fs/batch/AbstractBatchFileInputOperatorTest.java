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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.apex.api.operator.ControlTuple;
import org.apache.apex.malhar.lib.wal.FSWindowDataManager;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.Operator.ShutdownException;
import com.datatorrent.batch.BatchControlTuple;
import com.datatorrent.io.fs.utils.Scanner.SingleScanDirectoryScanner;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.util.TestUtils;
import com.datatorrent.moodi.lib.io.fs.batch.BatchBasedLineByLineFileInputOperator;
import com.datatorrent.testbench.CollectorTestSink;

public class AbstractBatchFileInputOperatorTest
{

  public static class TestMeta extends TestWatcher
  {
    public String dir = null;
    Context.OperatorContext context;

    @Override
    protected void starting(Description description)
    {
      TestUtils.deleteTargetTestClassFolder(description);
      String methodName = description.getMethodName();
      String className = description.getClassName();
      this.dir = "target/" + className + "/" + methodName;
      Attribute.AttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(Context.DAGContext.APPLICATION_PATH, dir);
      context = OperatorContextTestHelper.mockOperatorContext(1, attributes);
    }

    @Override
    protected void finished(Description description)
    {
      TestUtils.deleteTargetTestClassFolder(description);
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testBatchIdempotency() throws Exception
  {
    FileContext.getLocalFSFileContext().delete(new Path(new File(testMeta.dir).getAbsolutePath()), true);

    List<String> allLines = Lists.newArrayList();
    for (int file = 0; file < 2; file++) {
      List<String> lines = Lists.newArrayList();
      for (int line = 0; line < 2; line++) {
        lines.add("f" + file + "l" + line);
      }
      allLines.addAll(lines);
      FileUtils.write(new File(testMeta.dir, "file" + file), StringUtils.join(lines, '\n'));
    }

    BatchBasedLineByLineFileInputOperator oper = new BatchBasedLineByLineFileInputOperator();
    FSWindowDataManager manager = new FSWindowDataManager();
    manager.setStatePath(testMeta.dir + "/recovery");
    FSWindowDataManager controlDataManager = new FSWindowDataManager();
    controlDataManager.setStatePath(testMeta.dir + "/controlDataRecovery");

    oper.setWindowDataManager(manager);
    oper.setWindowControlDataManager(controlDataManager);

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    TestUtils.setSink(oper.output, queryResults);

    oper.setDirectory(testMeta.dir);

    oper.setup(testMeta.context);

    oper.getScanner().setFilePatternRegexp(".*file[\\d]");

    for (long wid = 0; wid < 4; wid++) {
      oper.beginWindow(wid);
      oper.emitTuples();
      oper.endWindow();
    }
    oper.teardown();
    List<String> beforeRecovery = Lists.newArrayList(queryResults.collectedTuples);
    List<ControlTuple> controlTuplesBeforeRecovery = Lists.newArrayList(queryResults.collectedControlTuples);

    queryResults.clear();

    //idempotency  part
    oper.setup(testMeta.context);
    for (long wid = 0; wid < 4; wid++) {
      oper.beginWindow(wid);
      oper.endWindow();
    }
    List<ControlTuple> controlTuplesAfterRecovery = Lists.newArrayList(queryResults.collectedControlTuples);

    Assert.assertEquals("number tuples", 4, queryResults.collectedTuples.size());
    Assert.assertEquals("lines", beforeRecovery, queryResults.collectedTuples);
    Assert.assertTrue(controlTuplesBeforeRecovery.get(0) instanceof BatchControlTuple.StartBatchControlTuple);
    Assert.assertTrue(controlTuplesBeforeRecovery.get(1) instanceof BatchControlTuple.EndBatchControlTuple);
    Assert.assertTrue(controlTuplesAfterRecovery.get(0) instanceof BatchControlTuple.StartBatchControlTuple);
    Assert.assertTrue(controlTuplesAfterRecovery.get(1) instanceof BatchControlTuple.EndBatchControlTuple);
    oper.teardown();
  }

  @Test
  public void testBatchIdempotencyWithCheckPoint() throws Exception
  {
    FileContext.getLocalFSFileContext().delete(new Path(new File(testMeta.dir).getAbsolutePath()), true);

    List<String> lines = Lists.newArrayList();
    int file = 0;
    for (int line = 0; line < 5; line++) {
      lines.add("f" + file + "l" + line);
    }
    FileUtils.write(new File(testMeta.dir, "file" + file), StringUtils.join(lines, '\n'));

    file = 1;
    lines = Lists.newArrayList();
    for (int line = 0; line < 6; line++) {
      lines.add("f" + file + "l" + line);
    }
    FileUtils.write(new File(testMeta.dir, "file" + file), StringUtils.join(lines, '\n'));

    // empty file
    file = 2;
    lines = Lists.newArrayList();
    FileUtils.write(new File(testMeta.dir, "file" + file), StringUtils.join(lines, '\n'));

    BatchBasedLineByLineFileInputOperator oper = new BatchBasedLineByLineFileInputOperator();
    FSWindowDataManager manager = new FSWindowDataManager();
    manager.setStatePath(testMeta.dir + "/recovery");
    FSWindowDataManager controlDataManager = new FSWindowDataManager();
    controlDataManager.setStatePath(testMeta.dir + "/controlDataRecovery");

    oper.setWindowDataManager(manager);
    oper.setWindowControlDataManager(controlDataManager);

    oper.setDirectory(testMeta.dir);

    // sort the pendingFiles and ensure the ordering of the files scanned
    DirectoryScannerNew newScanner = new DirectoryScannerNew();
    oper.setScanner(newScanner);
    oper.getScanner().setFilePatternRegexp(".*file[\\d]");

    oper.setup(testMeta.context);

    oper.setEmitBatchSize(3);

    //start batch emitted and scan directory
    oper.beginWindow(0);
    oper.emitTuples();
    oper.endWindow();

    // emit f0l0, f0l1, f0l2
    oper.beginWindow(1);
    oper.emitTuples();
    oper.endWindow();

    //checkpoint the operator
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    BatchBasedLineByLineFileInputOperator checkPointOper = checkpoint(oper, bos);

    // start saving output
    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    TestUtils.setSink(oper.output, queryResults);

    // emit f0l3, f0l4 and closeFile(f0) in the same window
    oper.beginWindow(2);
    oper.emitTuples();
    oper.endWindow();
    List<String> beforeRecovery2 = Lists.newArrayList(queryResults.collectedTuples);
    List<ControlTuple> beforeRecoveryControlTuples2 = Lists.newArrayList(queryResults.collectedControlTuples);

    // openfile(f1) and emit f1l0, f1l1, f1l2
    oper.beginWindow(3);
    oper.emitTuples();
    oper.endWindow();
    List<String> beforeRecovery3 = Lists.newArrayList(queryResults.collectedTuples);
    List<ControlTuple> beforeRecoveryControlTuples3 = Lists.newArrayList(queryResults.collectedControlTuples);

    // emit f1l3, f1l4, f1l5
    oper.beginWindow(4);
    oper.emitTuples();
    oper.endWindow();
    List<String> beforeRecovery4 = Lists.newArrayList(queryResults.collectedTuples);
    List<ControlTuple> beforeRecoveryControlTuples4 = Lists.newArrayList(queryResults.collectedControlTuples);

    //closeFile(f1) in a new window
    oper.beginWindow(5);
    oper.emitTuples();
    oper.endWindow();
    List<String> beforeRecovery5 = Lists.newArrayList(queryResults.collectedTuples);
    List<ControlTuple> beforeRecoveryControlTuples5 = Lists.newArrayList(queryResults.collectedControlTuples);

    // empty file ops, openFile(f2) and closeFile(f2) in emitTuples() only
    oper.beginWindow(6);
    oper.emitTuples();
    oper.endWindow();
    List<String> beforeRecovery6 = Lists.newArrayList(queryResults.collectedTuples);
    List<ControlTuple> beforeRecoveryControlTuples6 = Lists.newArrayList(queryResults.collectedControlTuples);

    // end batch only
    oper.beginWindow(7);
    oper.emitTuples();
    oper.endWindow();
    List<String> beforeRecovery7 = Lists.newArrayList(queryResults.collectedTuples);
    List<ControlTuple> beforeRecoveryControlTuples7 = Lists.newArrayList(queryResults.collectedControlTuples);

    oper.teardown();

    queryResults.clear();

    //idempotency  part

    oper = restoreCheckPoint(checkPointOper, bos);
    testMeta.context.getAttributes().put(Context.OperatorContext.ACTIVATION_WINDOW_ID, 1L);
    oper.setup(testMeta.context);
    TestUtils.setSink(oper.output, queryResults);

    long startwid = testMeta.context.getAttributes().get(Context.OperatorContext.ACTIVATION_WINDOW_ID);

    oper.beginWindow(++startwid);
    oper.emitTuples();
    oper.endWindow();
    Assert.assertEquals("lines", beforeRecovery2, queryResults.collectedTuples);
    Assert.assertEquals("No control tuples emitted", 0, beforeRecoveryControlTuples2.size());

    oper.beginWindow(++startwid);
    oper.emitTuples();
    oper.endWindow();
    Assert.assertEquals("lines", beforeRecovery3, queryResults.collectedTuples);
    Assert.assertEquals("No control tuples emitted", 0, beforeRecoveryControlTuples3.size());

    oper.beginWindow(++startwid);
    oper.emitTuples();
    oper.endWindow();
    Assert.assertEquals("lines", beforeRecovery4, queryResults.collectedTuples);
    Assert.assertEquals("No control tuples emitted", 0, beforeRecoveryControlTuples4.size());

    oper.beginWindow(++startwid);
    oper.emitTuples();
    oper.endWindow();
    Assert.assertEquals("lines", beforeRecovery5, queryResults.collectedTuples);
    Assert.assertEquals("No control tuples emitted", 0, beforeRecoveryControlTuples5.size());

    oper.beginWindow(++startwid);
    oper.emitTuples();
    oper.endWindow();
    Assert.assertEquals("lines", beforeRecovery6, queryResults.collectedTuples);
    Assert.assertEquals("No control tuples emitted", 0, beforeRecoveryControlTuples6.size());

    oper.beginWindow(++startwid);
    oper.emitTuples();
    oper.endWindow();
    Assert.assertEquals("lines", beforeRecovery7, queryResults.collectedTuples);
    Assert.assertTrue("End Batch control tuple",
        beforeRecoveryControlTuples7.get(0) instanceof BatchControlTuple.EndBatchControlTuple);

    Assert.assertEquals("number tuples", 8, queryResults.collectedTuples.size());

    oper.teardown();
  }

  @Test
  public void testBatchShutdown() throws Exception
  {
    FileContext.getLocalFSFileContext().delete(new Path(new File(testMeta.dir).getAbsolutePath()), true);

    List<String> allLines = Lists.newArrayList();
    for (int file = 0; file < 2; file++) {
      List<String> lines = Lists.newArrayList();
      for (int line = 0; line < 2; line++) {
        lines.add("f" + file + "l" + line);
      }
      allLines.addAll(lines);
      FileUtils.write(new File(testMeta.dir, "file" + file), StringUtils.join(lines, '\n'));
    }

    BatchBasedLineByLineFileInputOperator oper = new BatchBasedLineByLineFileInputOperator();
    FSWindowDataManager manager = new FSWindowDataManager();
    manager.setStatePath(testMeta.dir + "/recovery");
    FSWindowDataManager controlDataManager = new FSWindowDataManager();
    controlDataManager.setStatePath(testMeta.dir + "/controlDataRecovery");

    oper.setWindowDataManager(manager);
    oper.setWindowControlDataManager(controlDataManager);

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    TestUtils.setSink(oper.output, queryResults);

    oper.setDirectory(testMeta.dir);
    oper.setup(testMeta.context);

    oper.getScanner().setFilePatternRegexp(".*file[\\d]");

    for (long wid = 0; wid < 5; wid++) {
      oper.beginWindow(wid);
      oper.emitTuples();
      oper.endWindow();
    }
    boolean shutdownExceptionThrown = false;
    try {
      oper.committed(6);
    } catch (ShutdownException e) {
      shutdownExceptionThrown = true;
    }
    Assert.assertTrue("shutdown performed after committed", shutdownExceptionThrown);
    oper.teardown();
    List<String> beforeRecovery = Lists.newArrayList(queryResults.collectedTuples);
    List<ControlTuple> controlTuplesBeforeRecovery = Lists.newArrayList(queryResults.collectedControlTuples);

    queryResults.clear();

    //idempotency  part
    oper.setup(testMeta.context);
    for (long wid = 0; wid < 5; wid++) {
      oper.beginWindow(wid);
      oper.endWindow();
    }
    List<ControlTuple> controlTuplesAfterRecovery = Lists.newArrayList(queryResults.collectedControlTuples);

    Assert.assertEquals("number tuples", 4, queryResults.collectedTuples.size());
    Assert.assertEquals("lines", beforeRecovery, queryResults.collectedTuples);
    Assert.assertTrue(controlTuplesBeforeRecovery.get(0) instanceof BatchControlTuple.StartBatchControlTuple);
    Assert.assertTrue(controlTuplesBeforeRecovery.get(1) instanceof BatchControlTuple.EndBatchControlTuple);
    Assert.assertTrue(controlTuplesAfterRecovery.get(0) instanceof BatchControlTuple.StartBatchControlTuple);
    Assert.assertTrue(controlTuplesAfterRecovery.get(1) instanceof BatchControlTuple.EndBatchControlTuple);
    shutdownExceptionThrown = false;
    try {
      oper.committed(6);
    } catch (ShutdownException e) {
      shutdownExceptionThrown = true;
    }
    Assert.assertTrue("shutdown performed after committed after recovery", shutdownExceptionThrown);
    oper.teardown();
  }

  @SuppressWarnings("serial")
  private static class DirectoryScannerNew extends SingleScanDirectoryScanner
  {
    public LinkedHashSet<Path> scan(FileSystem fs, Path filePath, Set<String> consumedFiles)
    {
      LinkedHashSet<Path> pathSet;
      pathSet = super.scan(fs, filePath, consumedFiles);

      TreeSet<Path> orderFiles = new TreeSet<>();
      orderFiles.addAll(pathSet);
      pathSet.clear();
      Iterator<Path> fileIterator = orderFiles.iterator();
      while (fileIterator.hasNext()) {
        pathSet.add(fileIterator.next());
      }

      return pathSet;
    }
  }

  public static BatchBasedLineByLineFileInputOperator checkpoint(BatchBasedLineByLineFileInputOperator oper,
      ByteArrayOutputStream bos) throws Exception
  {
    Kryo kryo = new Kryo();

    Output loutput = new Output(bos);
    kryo.writeObject(loutput, oper);
    loutput.close();

    Input lInput = new Input(bos.toByteArray());
    BatchBasedLineByLineFileInputOperator checkPointedOper = kryo.readObject(lInput, oper.getClass());
    lInput.close();

    return checkPointedOper;
  }

  /**
   * Restores the checkpointed operator.
   * 
   * @param checkPointOper
   *          The checkpointed operator.
   * @param bos
   *          The ByteArrayOutputStream which saves the checkpoint data
   *          temporarily.
   */
  public static BatchBasedLineByLineFileInputOperator restoreCheckPoint(
      BatchBasedLineByLineFileInputOperator checkPointOper, ByteArrayOutputStream bos) throws Exception
  {
    Kryo kryo = new Kryo();

    Input lInput = new Input(bos.toByteArray());
    BatchBasedLineByLineFileInputOperator oper = kryo.readObject(lInput, checkPointOper.getClass());
    lInput.close();

    return oper;
  }
}
