package com.datatorrent.lib.schemaAware;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.schema.api.Schema;
import com.datatorrent.schema.api.SchemaExtended;

public class JsonParserTest
{
  static JsonParser parser = new JsonParser();
  static Map<OutputPort, Schema> outSchema = new HashMap<OutputPort, Schema>();
  static SchemaExtended schemaExtended = new SchemaExtended();
  static CollectorTestSink<Object> error = new CollectorTestSink<Object>();
  static CollectorTestSink<Object> pojoPort = new CollectorTestSink<Object>();
  static CollectorTestSink<Object> objectPort = new CollectorTestSink<Object>();

  @BeforeClass
  public static void setup()
  {
    parser.err.setSink(error);
    parser.out.setSink(pojoPort);
    parser.parsedOutput.setSink(objectPort);
    parser.setFieldInfo("{\"adId\":\"INTEGER\",\"adName\":\"STRING\",\"bidPrice\":\"DOUBLE\",\"active\":\"BOOLEAN\",\"weatherTargeted\":\"CHARACTER\"}");
    String name = "ad";
    schemaExtended.setName(name);
    schemaExtended.setFqcn(name);
    outSchema.put(parser.out, schemaExtended);
    parser.registerSchema(new HashMap<InputPort, Schema>(), outSchema);
    parser.setup(null);
  }

  @Test
  public void TestSchemaRegistration()
  {
    Assert.assertEquals(5, outSchema.get(parser.out).getFieldList().size());
    Assert.assertEquals(outSchema.get(parser.out).getField("adId"), Integer.class);
    Assert.assertEquals(outSchema.get(parser.out).getField("adName"), String.class);
    Assert.assertEquals(outSchema.get(parser.out).getField("bidPrice"), Double.class);
    Assert.assertEquals(outSchema.get(parser.out).getField("active"), Boolean.class);
    Assert.assertEquals(outSchema.get(parser.out).getField("weatherTargeted"), Character.class);
  }

  @Test
  public void TestPropogation() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException,
      NoSuchMethodException, SecurityException, IOException
  {

    schemaExtended.generateBean();
    Class clazz = schemaExtended.getBeanClass();
    parser.setClazz(clazz);

    String tuple = "{" + "\"adId\": 1234," + "\"adName\": \"adxyz\"," + "\"bidPrice\": 0.2," + "\"active\": \"true\","
        + "\"weatherTargeted\": \"Y\"" + "}";
    parser.beginWindow(0);
    parser.in.process(tuple.getBytes());
    parser.endWindow();
    Object obj = pojoPort.collectedTuples.get(0);

    Assert.assertEquals(1, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, objectPort.collectedTuples.size());
    Assert.assertEquals(0, error.collectedTuples.size());

    Assert.assertEquals(1234, clazz.getDeclaredMethod("getAdId").invoke(obj));
    Assert.assertEquals("adxyz", clazz.getDeclaredMethod("getAdName").invoke(obj));
    Assert.assertEquals(0.2, clazz.getDeclaredMethod("getBidPrice").invoke(obj));
    Assert.assertTrue((Boolean)clazz.getDeclaredMethod("getActive").invoke(obj));
    Assert.assertEquals('Y', clazz.getDeclaredMethod("getWeatherTargeted").invoke(obj));

  }

  @Test
  public void TestParserInvalidInput() throws IOException
  {
    schemaExtended.generateBean();
    Class clazz = schemaExtended.getBeanClass();
    parser.setClazz(clazz);

    String tuple = "{" + "\"adId\": \"invalidAdId\"," + "\"adName\": \"adxyz\"," + "\"bidPrice\": 0.2,"
        + "\"active\": \"true\"," + "\"weatherTargeted\": \"Y\"" + "}";
    parser.beginWindow(0);
    parser.in.process(tuple.getBytes());
    parser.endWindow();

    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
    KeyValPair<String, String> errorTuple = (KeyValPair<String, String>)error.collectedTuples.get(0);
    Assert.assertEquals(tuple, errorTuple.getKey());

  }

  @Test
  public void TestParserInvalidJson() throws IOException
  {
    schemaExtended.generateBean();
    Class clazz = schemaExtended.getBeanClass();
    parser.setClazz(clazz);

    String tuple = "\"adId\": \"invalidAdId\"," + "\"adName\": \"adxyz\"," + "\"bidPrice\": 0.2,"
        + "\"active\": \"true\"," + "\"weatherTargeted\": \"Y\"" + "}";
    parser.beginWindow(0);
    parser.in.process(tuple.getBytes());
    parser.endWindow();

    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
    KeyValPair<String, String> errorTuple = (KeyValPair<String, String>)error.collectedTuples.get(0);
    Assert.assertEquals(tuple, errorTuple.getKey());

  }

  @After
  public void clearSinks()
  {
    error.clear();
    objectPort.clear();
    pojoPort.clear();
  }

  @AfterClass
  public static void cleanUp()
  {
    parser.teardown();
  }

}
