package com.datatorrent.lib.schemaAware;

import java.util.Map;

import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.contrib.parser.DelimitedSchema;
import com.datatorrent.contrib.parser.DelimitedSchema.Field;
import com.datatorrent.schema.api.Schema;
import com.datatorrent.schema.api.SchemaAware;

/**
 * A schema aware CsvParser. This extends {@link CsvParser} and provides
 * implementation for registerSchema method to make it {@link SchemaAware}
 */
@InterfaceStability.Evolving
public class CsvParser extends com.datatorrent.contrib.parser.CsvParser implements SchemaAware
{
  /**
   * Adds information of fields to <b>outSchema</b> for <b>out</b> port of
   * {@link CsvParser}
   */
  public void registerSchema(Map<InputPort, Schema> inSchema, Map<OutputPort, Schema> outSchema)
  {
    if (outSchema.get(this.out) != null) {
      DelimitedSchema delimitedParserSchema = new DelimitedSchema(this.getSchema());
      for (Field field : delimitedParserSchema.getFields()) {
        outSchema.get(this.out).addField(field.getName(), SchemaAwareOperatorUtils.getClass(field.getType()));
      }
    }
  }
}
