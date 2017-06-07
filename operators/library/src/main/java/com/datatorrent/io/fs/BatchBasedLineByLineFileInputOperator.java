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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.apex.api.ControlAwareDefaultOutputPort;
import org.apache.hadoop.fs.Path;

import com.datatorrent.batch.BatchControlTuple;
import com.datatorrent.batch.EndApplicationControlTuple;

public class BatchBasedLineByLineFileInputOperator extends AbstractBatchFileInputOperator<String>
{
  public final transient ControlAwareDefaultOutputPort<String> output = new ControlAwareDefaultOutputPort<String>();

  protected transient BufferedReader br;

  @Override
  protected InputStream openFile(Path path) throws IOException
  {
    InputStream is = super.openFile(path);
    br = new BufferedReader(new InputStreamReader(is));
    return is;
  }

  @Override
  protected void closeFile(InputStream is) throws IOException
  {
    super.closeFile(is);
    br.close();
    br = null;
  }

  @Override
  protected String readEntity() throws IOException
  {
    return br.readLine();
  }

  @Override
  protected void emit(String tuple)
  {
    output.emit(tuple);
  }

  @Override
  public void emitStartBatchControlTuple()
  {
    BatchControlTuple startBatch = new BatchControlTuple.StartBatchControlTuple()
    {
      @Override
      public DeliveryType getDeliveryType()
      {
        return DeliveryType.IMMEDIATE;
      }
    };
    output.emitControl(startBatch);
  }

  @Override
  public void emitEndBatchControlTuple()
  {
    BatchControlTuple endBatch = new BatchControlTuple.EndBatchControlTuple()
    {
      @Override
      public DeliveryType getDeliveryType()
      {
        return DeliveryType.END_WINDOW;
      }
    };
    output.emitControl(endBatch);
  }

  @Override
  public void emitEndApplicationControlTuple()
  {
    output.emitControl(new EndApplicationControlTuple.EndApplicationControlTupleImpl());
  }
}
