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

import org.apache.apex.malhar.lib.fs.FSRecordReaderModule;
import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator.StringFileOutputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.schemaAware.CsvFormatter;
import com.datatorrent.lib.schemaAware.CsvParser;
import com.datatorrent.lib.schemaAware.FilterOperator;
import com.datatorrent.lib.schemaAware.TransformOperator;

@ApplicationAnnotation(name="HDFS-to-HDFS-Filter-Transform")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    FSRecordReaderModule recordReader = dag.addModule("recordReader", FSRecordReaderModule.class);
    CsvParser csvParser = dag.addOperator("csvParser", CsvParser.class);
    FilterOperator filterOperator = dag.addOperator("filter", new FilterOperator());
    TransformOperator transform = dag.addOperator("transform", new TransformOperator());
    CsvFormatter formatter = dag.addOperator("formatter", new CsvFormatter());
    StringFileOutputOperator fileOutput = dag.addOperator("fileOutput", new StringFileOutputOperator());

    dag.addStream("record", recordReader.records, csvParser.in);
    dag.addStream("pojo", csvParser.out, filterOperator.input);
    dag.addStream("filteredPojo", filterOperator.truePort, transform.input);
    dag.addStream("transformedPojo", transform.output, formatter.in);
    dag.addStream("string", formatter.out, fileOutput.input);

  }
}
