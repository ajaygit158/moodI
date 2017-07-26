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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.api.ControlAwareDefaultInputPort;
import org.apache.apex.api.operator.ControlTuple;

import com.datatorrent.api.StreamCodec;
import com.datatorrent.batch.BatchControlTuple;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

/**
 * This base implementation for a fault tolerant Batch based HDFS output operator,
 * which can handle outputting to multiple files when the output file depends on the tuple.
 * The file a tuple is output to is determined by the getFilePath method. The operator can
 * also output files to rolling mode. In rolling mode by default file names have '.#' appended to the
 * end, where # is an integer. A maximum length for files is specified and whenever the current output
 * file size exceeds the maximum length, output is rolled over to a new file whose name ends in '.#+1'.
 * After receiving the end batch control tuple, the operator will finalize all the open files.
 */
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
      LOG.debug("Received end batch");
      List<String> fileNameList = new ArrayList<>(this.getFileNameToTmpName().keySet());
      for (String fileName : fileNameList) {
        try {
          LOG.debug("Finalizing file : {}", fileName);
          finalizeFile(fileName);
        } catch (IOException e) {
          LOG.debug("Failed to finalize file {}", fileName);
          throw new RuntimeException(e);
        }
      }
    }
  }
}
