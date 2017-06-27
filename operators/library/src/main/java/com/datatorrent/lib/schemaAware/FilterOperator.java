package com.datatorrent.lib.schemaAware;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.annotations.VisibleForTesting;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.lib.expression.Expression;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.schema.api.Schema;
import com.datatorrent.schema.api.SchemaAware;

/**
 * Schema Aware Filter Operator
 */
@InterfaceStability.Evolving
public class FilterOperator extends com.datatorrent.lib.filter.FilterOperator implements SchemaAware
{
  public void registerSchema(Map<InputPort, Schema> inSchema, Map<OutputPort, Schema> outSchema)
  {
    Schema inputSchema = inSchema.get(input);
    for (Entry<String, Class> field : inputSchema.getFieldList().entrySet()) {
      if (outSchema.get(truePort) != null) {
        outSchema.get(truePort).addField(field.getKey(), field.getValue());
      }
      if (outSchema.get(falsePort) != null) {
        outSchema.get(falsePort).addField(field.getKey(), field.getValue());
      }
      if (outSchema.get(error) != null) {
        outSchema.get(error).addField(field.getKey(), field.getValue());
      }
    }
  }

  private String condition;
  private List<String> expressionFunctions;
  private List<String> additionalExpressionFunctions = new ArrayList<>();

  private transient Class<?> inClazz = null;
  private transient Expression<Boolean> expr = null;

  @AutoMetric
  private long trueTuples;

  @AutoMetric
  private long falseTuples;

  @AutoMetric
  private long totalFalseTuples;

  @AutoMetric
  private long errorTuples;

  public final transient DefaultOutputPort<Object> truePort = new DefaultOutputPort<Object>();

  public final transient DefaultOutputPort<Object> falsePort = new DefaultOutputPort<Object>();

  public final transient DefaultOutputPort<Object> error = new DefaultOutputPort<Object>();

  public FilterOperator()
  {
    expressionFunctions = new ArrayList<String>(Arrays.asList(new String[] {"java.lang.Math.*",
      "org.apache.commons.lang3.StringUtils.*", "org.apache.commons.lang3.StringEscapeUtils.*",
      "org.apache.commons.lang3.time.DurationFormatUtils.*", "org.apache.commons.lang3.time.DateFormatUtils.*" }));
  }

  @InputPortFieldAnnotation(schemaRequired = true)
  public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
  {
    public void setup(Context.PortContext context)
    {
      inClazz = context.getValue(Context.PortContext.TUPLE_CLASS);
    }

    @Override
    public void process(Object t)
    {
      processTuple(t);
    }
  };

  @Override
  public void activate(Context context)
  {
    createExpression();
  }

  @Override
  public void deactivate()
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
    errorTuples = trueTuples = falseTuples = 0;
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    totalFalseTuples += falseTuples;
  }

  /**
   * createExpression: create an expression from condition of POJO fields
   * Override this function for custom field expressions
   */
  protected void createExpression()
  {
    logger.info("Creating an expression for condition {}", condition);
    for (String expression : additionalExpressionFunctions) {
      if (expression != null) {
        expressionFunctions.add(expression);
      }
    }
    expr = PojoUtils.createExpression(inClazz, condition, Boolean.class,
      expressionFunctions.toArray(new String[expressionFunctions.size()]));
  }

  /**
   * evalExpression: Evaluate condition/expression
   * Override this function for custom condition evaluation
   */
  protected Boolean evalExpression(Object t)
  {
    return expr.execute(t);
  }

  /**
   * handleFilter: emit POJO meeting condition on truePort
   * and if it did not meet condition then on falsePort
   */
  private void processTuple(Object t)
  {
    try {
      if (evalExpression(t)) {
        truePort.emit(t);
        trueTuples++;
      } else {
        falsePort.emit(t);
        falseTuples++;
      }
    } catch (Exception ex) {
      logger.error("Error in expression eval: {}", ex.getMessage());
      logger.debug("Exception: ", ex);
      error.emit(t);
      errorTuples++;
    }
  }

  /**
   * Returns condition/expression with which Filtering is done
   *
   * @return condition parameter of Filter Operator
   */
  public String getCondition()
  {
    return condition;
  }

  /**
   * Set condition/expression with which Filtering operation would be applied
   *
   * @param condition parameter of Filter Operator
   */
  public void setCondition(String condition)
  {
    logger.info("Changing condition from {} to {}", this.condition, condition);
    this.condition = condition;
  }

  /**
   * Returns the list of expression function which would be made available to
   * expression to use. This is in addition to default expression functions
   * added by the operator
   *
   * @return List of functions available in expression.
   */
  public List<String> getAdditionalExpressionFunctions()
  {
    return additionalExpressionFunctions;
  }

  /**
   * Set list of import classes/method should should be made statically available
   * to expression to use.
   * For ex. org.apache.apex.test1.Test would mean that "Test" method will be
   * available in the expression to be used directly.
   * This is in addition to default expression functions added by the operator.
   * This is an optional property. See constructor to see defaults that are included.
   *
   * @param additionalExpressionFunctions List of qualified class/method that needs to be
   *                            imported to expression.
   */
  public void setAdditionalExpressionFunctions(List<String> additionalExpressionFunctions)
  {
    this.additionalExpressionFunctions = additionalExpressionFunctions;
  }

  public void setOptionalExpressionFunctionsItem(int index, String value)
  {
    final int need = index - additionalExpressionFunctions.size() + 1;
    for (int i = 0; i < need; i++) {
      additionalExpressionFunctions.add(null);
    }
    additionalExpressionFunctions.set(index, value);
  }

  @VisibleForTesting
  List<String> getExpressionFunctions()
  {
    return expressionFunctions;
  }

  private static final Logger logger = LoggerFactory.getLogger(com.datatorrent.lib.filter.FilterOperator.class);

}
