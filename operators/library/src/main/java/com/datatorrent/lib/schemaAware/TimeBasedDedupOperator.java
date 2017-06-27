package com.datatorrent.lib.schemaAware;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.schema.api.Schema;
import com.datatorrent.schema.api.SchemaAware;

/**
 * Schema Aware Dedup Operator
 */
@InterfaceStability.Evolving
public class TimeBasedDedupOperator extends org.apache.apex.malhar.lib.dedup.TimeBasedDedupOperator
    implements SchemaAware
{

  @Override
  public void registerSchema(Map<InputPort, Schema> inSchema, Map<OutputPort, Schema> outSchema)
  {

    Schema inputSchema = inSchema.get(input);
    for (Entry<String, Class> field : inputSchema.getFieldList().entrySet()) {
      if (outSchema.get(unique) != null) {
        outSchema.get(unique).addField(field.getKey(), field.getValue());
      }
      if (outSchema.get(duplicate) != null) {
        outSchema.get(duplicate).addField(field.getKey(), field.getValue());
      }
      if (outSchema.get(expired) != null) {
        outSchema.get(expired).addField(field.getKey(), field.getValue());
      }
    }
  }

}
