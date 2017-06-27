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

import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.cassandra.CassandraTransactionalStore;
import com.datatorrent.lib.schemaAware.CassandraOutputOperator;
import com.datatorrent.lib.schemaAware.FilterOperator;
import com.datatorrent.lib.schemaAware.JsonParser;
import com.datatorrent.lib.schemaAware.TransformOperator;

@ApplicationAnnotation(name = "Kafka-to-Cassandra-Filter-Transform")
public class Application implements StreamingApplication
{

  public void populateDAG(DAG dag, Configuration conf)
  {
    // This kafka input operator takes input from specified Kafka brokers.
    KafkaSinglePortInputOperator kafkaInputOperator = dag.addOperator("kafkaInput", KafkaSinglePortInputOperator.class);

    // Parses a json string tuple against a specified json schema and emits JSONObject.
    JsonParser jsonParser = dag.addOperator("jsonParser", new JsonParser());

    // Filters the tuple as per specified condition by user.
    FilterOperator filterOperator = dag.addOperator("filter", new FilterOperator());

    // Transforms the tuple value to user logic. Note logic may be modified.
    TransformOperator transform = dag.addOperator("transform", new TransformOperator());

    // Writes the data or Pojo to Cassandra database.
    CassandraTransactionalStore transactionalStore = new CassandraTransactionalStore();
    CassandraOutputOperator cassandraOutputOperator = dag.addOperator("cassandraOutput", new CassandraOutputOperator());
    cassandraOutputOperator.setStore(transactionalStore);

    // Now create the streams to complete the dag or application logic.
    dag.addStream("KafkaToJsonParser", kafkaInputOperator.outputPort, jsonParser.in);
    dag.addStream("JsonParserToFilter", jsonParser.out, filterOperator.input);
    dag.addStream("FilterToTransform", filterOperator.truePort, transform.input);
    dag.addStream("TransformToCassandraDB", transform.output, cassandraOutputOperator.input);
  }

}

