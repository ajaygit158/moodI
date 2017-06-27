package com.datatorrent.lib.schemaAware;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import org.apache.commons.beanutils.BeanUtils;

import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.schema.api.Schema;
import com.datatorrent.schema.api.SchemaExtended;

public class JsonFormatterTest
{

  static JsonFormatter formatter = new JsonFormatter();
  static SchemaExtended schema = new SchemaExtended();
  static Map<InputPort, Schema> inSchema = new HashMap<InputPort, Schema>();
  static CollectorTestSink<Object> error = new CollectorTestSink<Object>();
  static CollectorTestSink<Object> out = new CollectorTestSink<Object>();

  @BeforeClass
  public static void setup() throws IOException
  {
    String name = "ad";
    schema.setName(name);
    schema.setFqcn(name);
    schema.addField("adId", Integer.class);
    schema.addField("campId", Long.class);
    schema.addField("adName", String.class);
    schema.addField("active", Boolean.class);
    schema.generateBean();
    Class clazz = schema.getBeanClass();
    inSchema.put(formatter.in, schema);
    formatter.err.setSink(error);
    formatter.out.setSink(out);

    formatter.registerSchema(inSchema, new HashMap<OutputPort, Schema>());
    formatter.setClazz(clazz);
    formatter.setup(null);

  }

  @Test
  public void TestSchemaRegistration()
  {
    Assert.assertEquals(schema.getBeanClass().getDeclaredFields().length,
        inSchema.get(formatter.in).getFieldList().size());
  }

  @Test
  public void TestPropogation()
    throws InstantiationException, IOException, IllegalAccessException, InvocationTargetException, JSONException
  {

    Object object = formatter.getClazz().newInstance();
    BeanUtils.setProperty(object, "adId", 1234);
    BeanUtils.setProperty(object, "campId", 98233);
    BeanUtils.setProperty(object, "active", true);
    BeanUtils.setProperty(object, "adName", "adxyz");

    formatter.beginWindow(0);
    formatter.in.process(object);
    formatter.endWindow();

    Assert.assertEquals(1, out.collectedTuples.size());
    Assert.assertEquals(0, error.collectedTuples.size());
    String expectedOutput = "{\"campId\":98233,\"adId\":1234,\"active\":true,\"adName\":\"adxyz\"}";
    JSONAssert.assertEquals(expectedOutput, out.collectedTuples.get(0).toString().trim(), false);
  }

  @After
  public void clearSinks()
  {

    error.clear();
    out.clear();
  }

  @AfterClass
  public static void cleanUp()
  {
    formatter.teardown();
  }

}
