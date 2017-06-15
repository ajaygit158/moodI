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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Maps;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.enrich.JDBCLoader;
import com.datatorrent.contrib.enrich.JsonFSLoader;
import com.datatorrent.io.fs.BatchBasedLineByLineFileInputOperator;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.schemaAware.CsvParser;
import com.datatorrent.lib.schemaAware.FilterOperator;
import com.datatorrent.lib.schemaAware.POJOEnricher;
import com.datatorrent.lib.schemaAware.TransformOperator;
import com.datatorrent.operators.PojoBatchFileOutputOperator;
import com.datatorrent.operators.StringToByteConverter;

@ApplicationAnnotation(name = "BatchTelecomSolution")
public class Application implements StreamingApplication
{

  public void populateDAG(DAG dag, Configuration conf)
  {
    BatchBasedLineByLineFileInputOperator cdrReader = dag.addOperator("cdrReader", BatchBasedLineByLineFileInputOperator.class);
    StringToByteConverter byteConverter = dag.addOperator("byteConverter", StringToByteConverter.class);
    CsvParser csvParser = dag.addOperator("csvParser", CsvParser.class);
    csvParser.setSchema(SchemaUtils.jarResourceFileToString("csvSchema.json"));
    TransformOperator transformOperator = dag.addOperator("transformer", TransformOperator.class);
    FilterOperator callTypeFilterOperator = dag.addOperator("callTypeFilter", FilterOperator.class);
    FilterOperator planTypeFilterOperator = dag.addOperator("planTypeFilter", FilterOperator.class);
    POJOEnricher enrichDetailsCaller = dag.addOperator("FromContactEnrich", POJOEnricher.class);
    POJOEnricher enrichDetailsCallee = dag.addOperator("ToContactEnrich", POJOEnricher.class);
    JDBCLoader callerJdbcLoader = new JDBCLoader();
    enrichDetailsCaller.setStore(callerJdbcLoader);

    List<String> callerIncludeFields = new ArrayList<>();
    callerIncludeFields.add("region_id");
    callerIncludeFields.add("plan_type");
    callerIncludeFields.add("plan_id");
    enrichDetailsCaller.setIncludeFields(callerIncludeFields);
    List<String> callerLookupFields = new ArrayList<>();
    callerLookupFields.add("fromContactNo");
    enrichDetailsCaller.setLookupFields(callerLookupFields);
    Map<String, String> callerIncludeFieldsMap = Maps.newHashMap();
    enrichDetailsCaller.setIncludeFieldsMap(callerIncludeFieldsMap);
    callerIncludeFieldsMap.put("homeRegion", "INTEGER");
    callerIncludeFieldsMap.put("planType", "STRING");
    callerIncludeFieldsMap.put("planId", "INTEGER");

    JDBCLoader calleeJdbcLoader = new JDBCLoader();
    enrichDetailsCallee.setStore(calleeJdbcLoader);
    List<String> calleeIncludeFields = new ArrayList<>();
    calleeIncludeFields.add("region_id");
    calleeIncludeFields.add("toContactNo");
    enrichDetailsCallee.setIncludeFields(calleeIncludeFields);
    List<String> calleeLookupFields = new ArrayList<>();
    calleeLookupFields.add("toContactNo");
    enrichDetailsCallee.setLookupFields(calleeLookupFields);
    Map<String, String> includeFieldsMapCallee = Maps.newHashMap();
    enrichDetailsCallee.setIncludeFieldsMap(includeFieldsMapCallee);
    includeFieldsMapCallee.put("calleeRegion", "INTEGER");
    POJOEnricher planDetailsEnricher = dag.addOperator("PlanDetailsEnricher", POJOEnricher.class);
    JsonFSLoader fsLoader = new JsonFSLoader();
    planDetailsEnricher.setStore(fsLoader);
    List<String> planDetailsIncludeFields = new ArrayList<>();
    planDetailsIncludeFields.add("pulseType");
    planDetailsIncludeFields.add("localSmsCost");
    planDetailsIncludeFields.add("nationalSmsCost");
    planDetailsIncludeFields.add("localPulseCost");
    planDetailsIncludeFields.add("nationalPulseCost");
    planDetailsEnricher.setIncludeFields(planDetailsIncludeFields);
    List<String> planDetailsLookupFields = new ArrayList<>();
    planDetailsLookupFields.add("planType");
    planDetailsLookupFields.add("planId");
    planDetailsEnricher.setLookupFields(planDetailsLookupFields);
    Map<String, String> includeFieldsMapPlanDetails = Maps.newHashMap();
    planDetailsEnricher.setIncludeFieldsMap(includeFieldsMapPlanDetails);
    includeFieldsMapPlanDetails.put("pulseType", "STRING");
    includeFieldsMapPlanDetails.put("localSmsCost", "DOUBLE");
    includeFieldsMapPlanDetails.put("nationalSmsCost", "DOUBLE");
    includeFieldsMapPlanDetails.put("localPulseCost", "DOUBLE");
    includeFieldsMapPlanDetails.put("nationalPulseCost", "DOUBLE");
    TransformOperator costCalculator = dag.addOperator("costCalculator", TransformOperator.class);
    PojoBatchFileOutputOperator fileOutputOperator = dag.addOperator("fileOutputOperator", PojoBatchFileOutputOperator.class);

    dag.addStream("cdrRecordsString", cdrReader.output, byteConverter.input);
    dag.addStream("cdrRecordsByteArray", byteConverter.output, csvParser.in);
    dag.addStream("CdrPojoObjects", csvParser.out, callTypeFilterOperator.input);
    dag.addStream("payableCDRs", callTypeFilterOperator.truePort, enrichDetailsCaller.input);
    dag.addStream("callerDetailsEnrichedCDRs", enrichDetailsCaller.output, planTypeFilterOperator.input);
    dag.addStream("postpaidCallerDetailsEnrichedCDRs", planTypeFilterOperator.truePort, enrichDetailsCallee.input);
    dag.addStream("postPaidCalleeEnrichedCDRs", enrichDetailsCallee.output, planDetailsEnricher.input);
    dag.addStream("planDetailsEnrichedCDRs", planDetailsEnricher.output, transformOperator.input);
    dag.addStream("transformedCDRs", transformOperator.output, costCalculator.input);
    dag.addStream("costCalculatedCDRs", costCalculator.output, fileOutputOperator.input);
  }
}
