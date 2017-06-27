package com.datatorrent.lib.schemaAware;

import java.io.IOException;
import java.sql.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.validation.constraints.NotNull;

import org.apache.hadoop.classification.InterfaceStability;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.datatorrent.contrib.parser.Schema.FieldType;
import com.datatorrent.schema.api.Schema;
import com.datatorrent.schema.api.SchemaAware;

/**
 * Schema aware Transform Operator
 */
@InterfaceStability.Evolving
public class TransformOperator extends com.datatorrent.lib.transform.TransformOperator implements SchemaAware
{

  @NotNull
  private String outputFieldInfo;
  @NotNull
  private String expressionInfo;
  private transient Map<String, String> outputFieldMap = new HashMap<String, String>();

  @Override
  public void registerSchema(Map<InputPort, Schema> inSchema, Map<OutputPort, Schema> outSchema)
  {
    if (outSchema.get(output) != null) {
      for (Entry<String, String> field : outputFieldMap.entrySet()) {
        outSchema.get(output).addField(field.getKey(), getClass(FieldType.valueOf(field.getValue())));
      }
      if (this.isCopyMatchingFields()) {
        for (Entry<String, Class> field : inSchema.get(this.input).getFieldList().entrySet()) {
          outSchema.get(output).addField(field.getKey(), field.getValue());
        }
      }
    }
  }

  public String getOutputFieldInfo()
  {
    return outputFieldInfo;
  }

  /**
   * Set outputFieldInfo string (outputFieldName => fieldType) which defines
   * output fields and its data type. This is a mandatory property and is
   * specified in JSON format Possible values for data types are
   * BOOLEAN,DOUBLE,INTEGER,FLOAT,LONG,SHORT,CHARACTER,STRING,DATE
   * 
   * @param outputFieldInfo
   *          JSON string defining outputFieldName => fieldType.<br>
   *          E.g {"adId":"INTEGER","bidPrice":"DOUBLE"}
   */
  public void setOutputFieldInfo(String outputFieldInfo)
  {
    this.outputFieldInfo = outputFieldInfo;
    try {
      this.outputFieldMap = new ObjectMapper().readValue(this.outputFieldInfo, new TypeReference<Map<String, String>>()
      {
      });
    } catch (IOException e) {
      throw new RuntimeException("Not a valid JSON " + outputFieldInfo);
    }
  }

  /**
   * Returns expressionInfo String which defines outputFieldName => Expression
   * mapping in JSON format.
   *
   * @return JSON String representing outputFieldName => Expression
   */
  public String getExpressionInfo()
  {
    return expressionInfo;
  }

  /**
   * Set expressionInfo string (outputFieldName => Expression) which defines how
   * output POJO should be generated. This is a mandatory property and is
   * specified in JSON format
   * 
   * @param expressionInfo
   *          JSON string defining expression for output field.
   *
   * @description $(key) Output field for which expression should be evaluated
   * @description $(value) Expression to be evaluated for output field.
   * @useSchema $(key) output.fields[].name
   */
  public void setExpressionInfo(String expressionInfo)
  {
    this.expressionInfo = expressionInfo;
    try {
      this.setExpressionMap((Map<String, String>)new ObjectMapper().readValue(this.expressionInfo,
          new TypeReference<Map<String, String>>()
          {
          }));
    } catch (IOException e) {
      throw new RuntimeException("Not a valid JSON " + expressionInfo);
    }
  }

  private Class getClass(FieldType type)
  {
    switch (type) {
      case CHARACTER:
        return Character.class;
      case STRING:
        return String.class;
      case INTEGER:
        return Integer.class;
      case LONG:
        return Long.class;
      case SHORT:
        return Short.class;
      case DOUBLE:
        return Double.class;
      case FLOAT:
        return Float.class;
      case BOOLEAN:
        return Boolean.class;
      case DATE:
        return Date.class;
      default:
        return null;
    }
  }

}
