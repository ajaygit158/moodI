package com.datatorrent.operators;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

public class StringToByteConverter extends BaseOperator
{
  public final transient DefaultInputPort<String> input = new DefaultInputPort<String>()
  {

    @Override
    public void process(String tuple)
    {
      output.emit(tuple.getBytes());

    }
  };

  public final transient DefaultOutputPort<byte[]> output = new DefaultOutputPort<>();
}
