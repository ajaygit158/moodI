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
import com.datatorrent.fs.s3.S3RecordReaderWrapperModule;
import com.datatorrent.lib.converter.StringToByteArrayConverterOperator;
import com.datatorrent.lib.metrics.RedshiftOutputModule;
import com.datatorrent.lib.transform.TransformOperator;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Maps;

@ApplicationAnnotation(name="S3-to-redshift")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    //Add S3 as input and redshift as output operators to DAG
    S3RecordReaderWrapperModule inputModule = dag.addModule("S3Input", new S3RecordReaderWrapperModule());

    CsvParser csvParser = dag.addOperator("csvParser", CsvParser.class);
    TransformOperator transform = dag.addOperator("transform", new TransformOperator());
    Map<String, String> expMap = Maps.newHashMap();
    expMap.put("name", "{$.name}.toUpperCase()");
    transform.setExpressionMap(expMap);
    CsvFormatter formatter = dag.addOperator("formatter", new CsvFormatter());
    StringToByteArrayConverterOperator converterOp = dag.addOperator("converter", new StringToByteArrayConverterOperator());
    RedshiftOutputModule redshiftOutput = dag.addModule("RedshiftOutput", new RedshiftOutputModule());

    //Create streams
    dag.addStream("data", inputModule.records, csvParser.in);
    dag.addStream("pojo", csvParser.out, transform.input);
    dag.addStream("transformed", transform.output, formatter.in);
    dag.addStream("string", formatter.out, converterOp.input).setLocality(DAG.Locality.THREAD_LOCAL);
    dag.addStream("writeToJDBC", converterOp.output, redshiftOutput.input);

    dag.setAttribute(Context.DAGContext.METRICS_TRANSPORT, null);
  }
}
