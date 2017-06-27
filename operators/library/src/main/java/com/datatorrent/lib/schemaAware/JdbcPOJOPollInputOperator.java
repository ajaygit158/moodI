package com.datatorrent.lib.schemaAware;

import java.util.Map;

import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.lib.util.FieldInfo;
import com.datatorrent.schema.api.Schema;
import com.datatorrent.schema.api.SchemaAware;

/**
 * Schema aware JdbcInputOperator. This operator extends
 * {@link JdbcPOJOPollInputOperator} and provides implementation for register
 * schema method to make it {@link SchemaAware}
 */
@InterfaceStability.Evolving
public class JdbcPOJOPollInputOperator extends com.datatorrent.lib.db.jdbc.JdbcPOJOPollInputOperator
    implements SchemaAware
{

  /**
   * Add fields to <b>outSchema</b> for <b>outputPort</b> from {@link FieldInfo}
   */
  @Override
  public void registerSchema(Map<InputPort, Schema> inSchema, Map<OutputPort, Schema> outSchema)
  {
    if (outSchema.get(this.outputPort) != null) {
      for (FieldInfo fieldInfo : this.getFieldInfos()) {
        outSchema.get(this.outputPort).addField(fieldInfo.getPojoFieldExpression(), fieldInfo.getType().getJavaType());
      }
    }
  }
}
