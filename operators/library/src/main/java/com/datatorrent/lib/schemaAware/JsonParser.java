package com.datatorrent.lib.schemaAware;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceStability;

import com.esotericsoftware.kryo.NotNull;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.contrib.parser.Schema.FieldType;
import com.datatorrent.schema.api.Schema;
import com.datatorrent.schema.api.SchemaAware;

@InterfaceStability.Evolving
public class JsonParser extends com.datatorrent.contrib.parser.JsonParser implements SchemaAware
{
  /**
   * String representing fields in the data and their type in Json Format<br>
   * {"adId":"INTEGER","adName":"STRING","bidPrice":"DOUBLE"}<br>
   * Possible values for data types are
   * <b>BOOLEAN,DOUBLE,INTEGER,FLOAT,LONG,SHORT,CHARACTER,STRING,DATE</b>
   */
  @NotNull
  private String fieldInfo;
  private transient Map<String, String> fieldInfoMap = new HashMap<String, String>();

  @AutoMetric
  private long totalErrorTuples;

  /**
   * Adds information of fields to <b>outSchema</b> for <b>out</b> port of
   * {@link JsonParser}
   */
  public void registerSchema(Map<InputPort, Schema> inSchema, Map<OutputPort, Schema> outSchema)
  {
    if (outSchema.get(this.out) != null) {
      for (String field : fieldInfoMap.keySet()) {
        outSchema.get(this.out).addField(field,
            SchemaAwareOperatorUtils.getClass(FieldType.valueOf(fieldInfoMap.get(field))));
      }
    }
  }

  public String getFieldInfo()
  {
    return fieldInfo;
  }

  /**
   * Set the field info
   * 
   * @param fieldInfo
   *          String representing fields in the data and their type in Json
   *          Format<br>
   *          {"adId":"INTEGER","adName":"STRING","bidPrice":"DOUBLE"}<br>
   *          Possible values for data types are
   *          <b>BOOLEAN,DOUBLE,INTEGER,FLOAT,LONG,SHORT,CHARACTER,STRING,DATE
   *          </b>
   */
  public void setFieldInfo(String fieldInfo)
  {
    this.fieldInfo = fieldInfo;
    try {
      this.fieldInfoMap = new ObjectMapper().readValue(this.fieldInfo, new TypeReference<Map<String, String>>()
      {
      });
    } catch (IOException e) {
      throw new RuntimeException("Not a valid JSON " + fieldInfo);
    }
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    totalErrorTuples += errorTupleCount;
  }
}
