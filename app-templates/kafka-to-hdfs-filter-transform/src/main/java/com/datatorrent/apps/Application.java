/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.datatorrent.apps;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.schemaAware.FilterOperator;
import com.datatorrent.lib.schemaAware.JsonFormatter;
import com.datatorrent.lib.schemaAware.JsonParser;
import com.datatorrent.lib.schemaAware.TransformOperator;
import com.datatorrent.moodi.io.fs.StringFileOutputOperator;
import com.datatorrent.moodi.kafka.KafkaSinglePortInputOperator;

@ApplicationAnnotation(name = "Kafka-to-HDFS-Filter-Transform")
public class Application implements StreamingApplication
{

  public void populateDAG(DAG dag, Configuration conf)
  {
    KafkaSinglePortInputOperator kafkaInputOperator = dag.addOperator("kafkaInput", KafkaSinglePortInputOperator.class);
    JsonParser parser = dag.addOperator("parser", new JsonParser());
    TransformOperator transform = dag.addOperator("transform", new TransformOperator());
    FilterOperator filterOperator = dag.addOperator("filter", new FilterOperator());
    JsonFormatter formatter = dag.addOperator("formatter", new JsonFormatter());
    StringFileOutputOperator fileOutput = dag.addOperator("fileOutput", new StringFileOutputOperator());

    dag.addStream("data", kafkaInputOperator.outputPort, parser.in);
    dag.addStream("pojo", parser.out, filterOperator.input);
    dag.addStream("filtered", filterOperator.truePort, transform.input);
    dag.addStream("transformed", transform.output, formatter.in);
    dag.addStream("string", formatter.out, fileOutput.input);

    dag.setAttribute(Context.DAGContext.METRICS_TRANSPORT, null);
  }

}
