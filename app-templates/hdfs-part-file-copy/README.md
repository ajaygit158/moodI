### Description
The HDFS to HDFS part file copy Application Template continuously ingest files as blocks from one Hadoop cluster to another for backup or further processing without worrying about the compaction.
- The application keeps the same data at destination but creates part files instead of complete file. 
- The application scales linearly with number of block readers.
- The application is fault tolerant and can withstand node and cluster outages without data loss.
- The application is also highly performant and can perform as fast as the network bandwidth allows.
- The only configuration user needs to provide is source and destination HDFS paths.
- This enterprise grade application template will dramatically reduce your time to market and cost of operations.

Import the application from DataTorrent AppHub and follow the tutorial videos or walkthrough document below to launch the template and run it.

### Quickstart
Import and run the application template as an operable proof of concept. Please watch the [walkthrough video](Update link location here) to import and launch the application.

<iframe src="Update link location here" allowfullscreen="allowfullscreen" class="video" id="basicVideo" ga-track="basicVideo"></iframe>

### Logical Plan

Here is a preview of the logical plan of the application template

![Logical Plan](https://www.datatorrent.com/wp-content/uploads/2017/06/HDFS_Part_File_Copy_DAG.png)

### Launch App Properties

Here is a preview of the properties to be set at application launch

![Launch App Properties](https://www.datatorrent.com/wp-content/uploads/2017/06/HDFS_Part_File_Copy_Properties.png)

### Resources

Please find the walkthrough docs for app template as follows:

&nbsp; <a href="http://docs.datatorrent.com/app-templates/hdfs-parallel-file-copy/"  class="docs" id="docs" ga-track="docs" target="_blank">http://docs.datatorrent.com/app-templates/hdfs-parallel-file-copy/</a>

Please find the GitHub URL for app template as follows:

&nbsp; <a href="https://github.com/DataTorrent/moodI/tree/master/app-templates/hdfs-part-file-copy"  class="github" id="github" ga-track="github" target="_blank">https://github.com/DataTorrent/moodI/tree/master/app-templates/hdfs-part-file-copy</a>

Please send feedback or feature requests to:

&nbsp; <a href="mailto:feedback@datatorrent.com"  class="feedback" id="feedback" ga-track="feedback">feedback@datatorrent.com</a>

Please join the user discussion groups:

&nbsp; <a href="mailto:dt-users@googlegroups.com"  class="maillist" id="maillist" ga-track="maillist">dt-users@googlegroups.com</a>
