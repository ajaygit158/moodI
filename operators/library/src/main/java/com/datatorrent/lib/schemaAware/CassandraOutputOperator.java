package com.datatorrent.lib.schemaAware;

import java.util.Map;

import com.datatorrent.contrib.cassandra.CassandraPOJOOutputOperator;
import com.datatorrent.schema.api.Schema;
import com.datatorrent.schema.api.SchemaAware;

public class CassandraOutputOperator extends CassandraPOJOOutputOperator implements SchemaAware
{

  @Override
  public void registerSchema(Map<InputPort, Schema> arg0, Map<OutputPort, Schema> arg1)
  {
    // TODO Auto-generated method stub
    
  }

}
