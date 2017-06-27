package com.datatorrent.lib.schemaAware;

import java.util.Map;

import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.schema.api.Schema;
import com.datatorrent.schema.api.SchemaAware;

@InterfaceStability.Evolving
public class CsvFormatter extends com.datatorrent.contrib.formatter.CsvFormatter implements SchemaAware
{

  /**
   * Delimiter separating the values of the fields. Default is comma(,)
   */
  private Character separator = ',';
  /**
   * Comma separated String representing names of the fields that the operator
   * would receive on input port. The output port would emit the string of
   * values in the given order.
   */
  private String fieldOrder = "";

  private static final String schemaFormat = "{\n" + "      \"separator\": \"%s\",\n"
      + "      \"quoteChar\": \"\\\"\",\n" + "      \"fields\": [%s]\n" + "      }";
  private static final String fieldFormat = "{\n" + "      \"name\": \"%s\",\n" + "      \"type\": \"%s\"\n"
      + "      }";

  public void registerSchema(Map<InputPort, Schema> inSchema, Map<OutputPort, Schema> outSchema)
  {
    if (getSchema() != null) {
      return;
    }

    String fieldSchemaPart = "";

    boolean first = true;
    Map<String, Class> fieldList = inSchema.get(in).getFieldList();

    if (fieldOrder.length() == 0) {
      for (String s : fieldList.keySet()) {
        fieldOrder += s + ",";
      }
    }

    for (String fieldName : fieldOrder.split(",")) {
      if (!fieldList.containsKey(fieldName)) {
        continue;
      }

      Class fieldType = fieldList.get(fieldName);
      if (first) {
        first = false;
      } else {
        fieldSchemaPart += ",";
      }
      fieldSchemaPart += String.format(fieldFormat, fieldName, refactor(fieldType.getSimpleName()));
    }

    String schema = String.format(schemaFormat, separator, fieldSchemaPart);

    setSchema(schema);
  }

  private static String refactor(String simpleName)
  {
    if (simpleName.equalsIgnoreCase("int")) {
      return "Integer";
    }
    if (simpleName.equalsIgnoreCase("char")) {
      return "Character";
    }
    return simpleName;
  }

  /**
   * Get the delimeter character
   * 
   * @return the delimeter character
   */
  public Character getSeparator()
  {
    return separator;
  }

  /**
   * Set the delimiter character for output string
   * 
   * @param separator
   *          Delimiter character for output string
   */
  public void setSeparator(Character separator)
  {
    this.separator = separator;
  }

  public String getFieldOrder()
  {
    return fieldOrder;
  }

  /**
   * Set the field order
   * 
   * @param fieldOrder
   *          Comma separated String representing names of the fields that the
   *          operator would receive on input port. The output port would emit
   *          the string of values in the given order.
   */
  public void setFieldOrder(String fieldOrder)
  {
    this.fieldOrder = fieldOrder;
  }
}
