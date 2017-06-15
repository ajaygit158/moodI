### Description
The Kafka to HDFS Filter with Transform Application continuously reads json string messages from configured kafka topic(s), filters based on the filter criteria, applies given transformations on the specified fields and writes each message as a json string in HDFS file(s). 

To customize this application, user can configure it to run it with different schema at the launch time. 
- The application scales linearly with the number of Kafka brokers and Kafka topics.
- The application is fault tolerant and can withstand node and cluster outages without data loss.
- The application is also highly performant and can process faster than Kafka broker can produce per topic.
- It is extremely easy to add custom logic to get your business value without worrying about connectivity and operational details of Kafka consumer and HDFS writer.
- The only configuration user needs to provide is source Kafka broker list, topic list, input schema, filter criteria and destination HDFS path, filename.
- This enterprise grade application template will dramatically reduce your time to market and cost of operations.

Import the application from DataTorrent AppHub and launch it for the desired functionality. Follow the tutorial videos or walkthrough document below to launch the template and add custom logic to process the data during ingestion.

### Quickstart
Import and run application template as an operable proof of concept. Please watch the [walkthrough video](https://www.youtube.com/watch?v=taHc_QJUfBg) to import and launch the application.

<iframe src="https://basic-video-link?enablejsapi=1" allowfullscreen="allowfullscreen" class="video" id="basicVideo" ga-track="basicVideo"></iframe>

### Productize
Add custom logic to the application template and launch. Please watch the [walkthrough video](https://www.youtube.com/watch?v=S6CB4XgRHCE) to add custom logic to the application template.

<iframe src="https://advanced-video-link?enablejsapi=1" allowfullscreen="allowfullscreen" class="video" id="advancedVideo" ga-track="advancedVideo"></iframe>

### Logical Plan

Here is a preview of the logical plan of the application template

![Logical Plan](https://logical-dag.png)

### Launch App Properties

Here is a preview of the properties to be set at application launch

![Launch App Properties](https://app-launch-properties.png)

### Resources

Please find the walkthrough docs for app template as follows:

&nbsp; <a href="http://docs.datatorrent.com/app-templates/"  class="docs" id="docs" ga-track="docs" target="_blank">http://docs.datatorrent.com/app-templates/</a>

Please find the GitHub URL for app template as follows:

&nbsp; <a href="https://github.com/DataTorrent/moodI/tree/master/app-templates/kafka-to-hdfs-filter-transform"  class="github" id="github" ga-track="github" target="_blank">https://github.com/DataTorrent/moodI/tree/master/app-templates/kafka-to-hdfs-filter-transform</a>

Please send feedback or feature requests to:

&nbsp; <a href="mailto:feedback@datatorrent.com"  class="feedback" id="feedback" ga-track="feedback">feedback@datatorrent.com</a>

Please join the user discussion groups:

&nbsp; <a href="mailto:dt-users@googlegroups.com"  class="maillist" id="maillist" ga-track="maillist">dt-users@googlegroups.com</a>
