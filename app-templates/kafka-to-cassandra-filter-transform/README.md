### Description
The Kafka to Cassandra transform application ingest messages in JSON string format from configured kafka topic, transforms the data and writes each message as a record in Cassandra Database store. This application uses PojoEvent as an example schema, this can be customized to use custom schema based on specific needs.

- The application is fault tolerant and can withstand node and cluster outages without data loss.
- The application is also highly performant and can process as fast as the network allows.
- It is extremely easy to add custom logic to get your business value without worrying about database connectivity and operational details of database writer.
- The only configuration user needs to provide is source kafka broker lists and cassandra database details like node, table and keyspace.
- This enterprise grade application template will dramatically reduce your time to market and cost of operations.

Import the application from DataTorrent AppHub and launch it to ingest messages in JSON string format from configured kafka topic and writes each message as a record in cassandra store with transformation. Follow the tutorial videos or walkthrough document below to launch the template and add custom logic to process the data during ingestion.

### Quickstart
Import and run application template as an operable proof of concept. Please watch the [walkthrough video](https://www.youtube.com/watch?v=) for more details.

<iframe src="https://www.youtube.com/embed" allowfullscreen="allowfullscreen" class="video" id="basicVideo" ga-track="basicVideo"></iframe>

### Productize
Add custom logic to the application template and launch. Please watch the [walkthrough video](https://www.youtube.com/watch) to add custom logic to the application template.

<iframe src="https://www.youtube.com/embed/" allowfullscreen="allowfullscreen" class="video" id="advancedVideo" ga-track="advancedVideo"></iframe>

### Logical Plan

Here is a preview of the logical plan of the application template

![Logical Plan](https://www.datatorrent.com/wp-content/uploads/2016/12/)

### Launch App Properties

Here is a preview of the properties to be set at application launch

![Launch App Properties](https://www.datatorrent.com/wp-content/uploads/2016/12/)

### Resources

For [Walkthrough documentation](http://docs.datatorrent.com/app-templates/kafka-to-cassandra-transform) refer below link:

&nbsp; <a href="http://docs.datatorrent.com/app-templates/kafka-to-cassandra-transform"  class="docs" id="docs" ga-track="docs" target="_blank">http://docs.datatorrent.com/app-templates/kafka-to-cassandra-transform</a>

For [GitHub URL](http://docs.datatorrent.com/app-templates/kafka-to-cassandra-transform) refer below link:

&nbsp; <a href="https://github.com/DataTorrent/moodI/tree/master/app-templates/kafka-to-cassandra-transform"  class="github" id="github" ga-track="github" target="_blank">https://github.com/DataTorrent/moodI/tree/master/app-templates/kafka-to-cassandra-transform</a>

Please send feedback or feature requests to:

&nbsp; <a href="mailto:feedback@datatorrent.com"  class="feedback" id="feedback" ga-track="feedback">feedback@datatorrent.com</a>

Please join the user discussion groups:

&nbsp; <a href="mailto:dt-users@googlegroups.com"  class="maillist" id="maillist" ga-track="maillist">dt-users@googlegroups.com</a>
