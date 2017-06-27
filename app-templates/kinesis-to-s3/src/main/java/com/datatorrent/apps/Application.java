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

import java.util.Map;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.formatter.CsvFormatter;
import com.datatorrent.contrib.parser.CsvParser;
import com.datatorrent.lib.metrics.S3MetricsTupleOutputModule;
import com.datatorrent.lib.transform.TransformOperator;
import com.datatorrent.metrics.KinesisByteArrayInputOperator;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Maps;

@ApplicationAnnotation(name="kinesis-to-S3")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Add Kinesis as input and S3 as output operators respectively to dag.
    KinesisByteArrayInputOperator inputModule = dag.addOperator("KinesisInput", new KinesisByteArrayInputOperator());
    S3MetricsTupleOutputModule.S3BytesOutputModule outputModule = dag.addModule("S3OutputModule", new S3MetricsTupleOutputModule.S3BytesOutputModule());
    // Create a stream for messages from Kinesis to S3
    dag.addStream("KinesisToS3", inputModule.outputPort, outputModule.input);

    dag.setAttribute(Context.DAGContext.METRICS_TRANSPORT, null);

    /*
     * To add custom logic to your DAG, add your custom operator here with
     * dag.addOperator api call and connect it in the dag using the dag.addStream
     * api call.
     *
     * For example:
     *
     * To parse incoming csv lines, transform them and outputting them on kafka.
     * KinesisInput->CSVParser->Transform->CSVFormatter->S3OutputModule can be achieved as follows
     *
     * Adding operators:
     * CsvParser csvParser = dag.addOperator("csvParser", CsvParser.class);
     *
     * TransformOperator transform = dag.addOperator("transform", new TransformOperator());
     * Map<String, String> expMap = Maps.newHashMap();
     * expMap.put("name", "{$.name}.toUpperCase()");
     * transform.setExpressionMap(expMap);
     * CsvFormatter formatter = dag.addOperator("formatter", new CsvFormatter());
     *
     * Use S3TupleOutputModule.S3StringOutputModule instead of S3TupleOutputModule.S3BytesOutputModule i.e.
     * Replace the following line below:
     * S3TupleOutputModule.S3BytesOutputModule outputModule = dag.addModule("S3OutputModule", new S3TupleOutputModule.S3BytesOutputModule());
     * with this lines:
     * S3TupleOutputModule.S3StringOutputModule outputModule = dag.addModule("S3OutputModule", new S3TupleOutputModule.S3StringOutputModule());
     *
     * Connect these operators with appropriate streams
     * Replace the following line below:
     * dag.addStream("KinesisToS3", inputModule.outputPort, outputModule.input);
     *
     * with these lines:
     * dag.addStream("data", inputModule.outputPort, csvParser.in);
     * dag.addStream("pojo", csvParser.out, transform.input);
     * dag.addStream("transformed", transform.output, formatter.in);
     * dag.addStream("string", formatter.out, outputModule.input);
     *
     */
  }
}
