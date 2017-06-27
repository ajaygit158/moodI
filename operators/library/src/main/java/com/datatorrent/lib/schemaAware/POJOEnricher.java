package com.datatorrent.lib.schemaAware;

import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceStability;

import com.esotericsoftware.kryo.NotNull;

import com.datatorrent.contrib.parser.Schema.FieldType;
import com.datatorrent.schema.api.Schema;
import com.datatorrent.schema.api.SchemaAware;

/**
 * Schema aware EnrichOperator
 *
 */
@InterfaceStability.Evolving
public class POJOEnricher extends com.datatorrent.contrib.enrich.POJOEnricher implements SchemaAware
{

  @NotNull
  private Map<String, String> includeFieldsMap;

  public void registerSchema(Map<InputPort, Schema> inSchema, Map<OutputPort, Schema> outSchema)
  {
    if (outSchema.get(this.output) != null) {
      this.includeFields = new ArrayList<String>();
      this.includeFields.addAll(includeFieldsMap.keySet());
      for (Entry<String, Class> entry : inSchema.get(this.input).getFieldList().entrySet()) {
        outSchema.get(this.output).addField(entry.getKey(), entry.getValue());
      }
      for (Entry<String, String> entry : includeFieldsMap.entrySet()) {
        outSchema.get(this.output).addField(entry.getKey(), getClass(FieldType.valueOf(entry.getValue())));
      }
    }
    if (outSchema.get(this.error) != null) {
      for (Entry<String, Class> entry : inSchema.get(this.input).getFieldList().entrySet()) {
        outSchema.get(this.error).addField(entry.getKey(), entry.getValue());
      }
    }
  }

  /**
   * Returns includeFields --> field type map
   * 
   * @return includeFieldsMap
   */
  public Map<String, String> getIncludeFieldsMap()
  {
    return includeFieldsMap;
  }

  /**
   * Sets the map containing fields to be included with their type
   * 
   * @param includeFieldsMap
   *          Map of field Name -> field Type. E.g. <name,string>,
   *          <age,INTEGER> Field types can be one of
   *          <b> STRING,CHARACTER,INTEGER,LONG,SHORT,DOUBLE,FLOAT,BOOLEAN,DATE
   *          </b>.
   */
  public void setIncludeFieldsMap(Map<String, String> includeFieldsMap)
  {
    this.includeFieldsMap = includeFieldsMap;
  }

  private Class<?> getClass(FieldType type)
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
