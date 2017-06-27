package com.datatorrent.lib.converter;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * This operator converts String to Byte array
 */
public class StringToByteArrayConverterOperator extends BaseOperator implements Converter<String, byte[]>
{
  @Override
  public byte[] convert(String s)
  {
    return s.getBytes();
  }

  //Input port which accepts String
  public final transient DefaultInputPort<String> input = new DefaultInputPort<String>()
  {
    @Override
    public void process(String tuple)
    {
      output.emit(convert(tuple));
    }
  };

  public final transient DefaultOutputPort<byte[]> output = new DefaultOutputPort<>();
}
