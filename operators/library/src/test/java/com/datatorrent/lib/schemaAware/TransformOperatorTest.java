package com.datatorrent.lib.schemaAware;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.schema.api.Schema;

public class TransformOperatorTest
{
  static TransformOperator transform = new TransformOperator();
  static Map<InputPort, Schema> inSchema = new HashMap<InputPort, Schema>();
  static Map<OutputPort, Schema> outSchema = new HashMap<OutputPort, Schema>();

  @BeforeClass
  public static void setup() throws IOException
  {
    inSchema.put(transform.input, new Schema());
    outSchema.put(transform.output, new Schema());
    Map<String, String> outputFieldMap = new HashMap<String, String>();
    outputFieldMap.put("uniqueId", "STRING");
    transform.setOutputFieldInfo("{\"uniqueId\":\"STRING\"}");
    transform.registerSchema(inSchema, outSchema);
    transform.setup(null);
  }

  @Test
  public void TestSchemaRegistration()
  {
    Assert.assertEquals(1, outSchema.get(transform.output).getFieldList().size());
    Assert.assertEquals(String.class, outSchema.get(transform.output).getField("uniqueId"));
  }

}
