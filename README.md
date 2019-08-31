# Mobius development is deprecated and has been superseded by a more recent version '.NET for Apache Spark' from Microsoft ([Website](https://dot.net/spark) | [GitHub](https://github.com/dotnet/spark)) that runs on Azure HDInsight Spark, Amazon EMR Spark, Azure & AWS Databricks.

<img src='logo/mobius-star-200.png' width='125px' alt='Mobius logo' />

# Mobius: C# API for Spark

[Mobius](https://github.com/Microsoft/Mobius) provides C# language binding to [Apache Spark](https://spark.apache.org/) enabling the implementation of Spark driver program and data processing operations in the languages supported in the .NET framework like C# or F#.

For example, the word count sample in Apache Spark can be implemented in C# as follows :

```c#
var lines = sparkContext.TextFile(@"hdfs://path/to/input.txt");  
var words = lines.FlatMap(s => s.Split(' '));
var wordCounts = words.Map(w => new Tuple<string, int>(w.Trim(), 1))  
                      .ReduceByKey((x, y) => x + y);  
var wordCountCollection = wordCounts.Collect();  
wordCounts.SaveAsTextFile(@"hdfs://path/to/wordcount.txt");  
```

A simple DataFrame application using TempTable may look like the following:

```c#
var reqDataFrame = sqlContext.TextFile(@"hdfs://path/to/requests.csv");
var metricDataFrame = sqlContext.TextFile(@"hdfs://path/to/metrics.csv");
reqDataFrame.RegisterTempTable("requests");
metricDataFrame.RegisterTempTable("metrics");
// C0 - guid in requests DataFrame, C3 - guid in metrics DataFrame  
var joinDataFrame = GetSqlContext().Sql(  
    "SELECT joinedtable.datacenter" +
         ", MAX(joinedtable.latency) maxlatency" +
         ", AVG(joinedtable.latency) avglatency " +
    "FROM (" +
       "SELECT a.C1 as datacenter, b.C6 as latency " +  
       "FROM requests a JOIN metrics b ON a.C0  = b.C3) joinedtable " +   
    "GROUP BY datacenter");
joinDataFrame.ShowSchema();
joinDataFrame.Show();
```

A simple DataFrame application using DataFrame DSL may look like the following:

```  c#
// C0 - guid, C1 - datacenter
var reqDataFrame = sqlContext.TextFile(@"hdfs://path/to/requests.csv")  
                             .Select("C0", "C1");    
// C3 - guid, C6 - latency   
var metricDataFrame = sqlContext.TextFile(@"hdfs://path/to/metrics.csv", ",", false, true)
                                .Select("C3", "C6"); //override delimiter, hasHeader & inferSchema
var joinDataFrame = reqDataFrame.Join(metricDataFrame, reqDataFrame["C0"] == metricDataFrame["C3"])
                                .GroupBy("C1");
var maxLatencyByDcDataFrame = joinDataFrame.Agg(new Dictionary<string, string> { { "C6", "max" } });
maxLatencyByDcDataFrame.ShowSchema();
maxLatencyByDcDataFrame.Show();
```

A simple Spark Streaming application that processes messages from Kafka using C# may be implemented using the following code:

```  c#
StreamingContext sparkStreamingContext = StreamingContext.GetOrCreate(checkpointPath, () =>
    {
      var ssc = new StreamingContext(sparkContext, slideDurationInMillis);
      ssc.Checkpoint(checkpointPath);
      var stream = KafkaUtils.CreateDirectStream(ssc, topicList, kafkaParams, perTopicPartitionKafkaOffsets);
      //message format: [timestamp],[loglevel],[logmessage]
      var countByLogLevelAndTime = stream
                                    .Map(kvp => Encoding.UTF8.GetString(kvp.Value))
                                    .Filter(line => line.Contains(","))
                                    .Map(line => line.Split(','))
                                    .Map(columns => new Tuple<string, int>(
                                                          string.Format("{0},{1}", columns[0], columns[1]), 1))
                                    .ReduceByKeyAndWindow((x, y) => x + y, (x, y) => x - y,
                                                          windowDurationInSecs, slideDurationInSecs, 3)
                                    .Map(logLevelCountPair => string.Format("{0},{1}",
                                                          logLevelCountPair.Key, logLevelCountPair.Value));
      countByLogLevelAndTime.ForeachRDD(countByLogLevel =>
      {
          foreach (var logCount in countByLogLevel.Collect())
              Console.WriteLine(logCount);
      });
      return ssc;
    });
sparkStreamingContext.Start();
sparkStreamingContext.AwaitTermination();
```
For more code samples, refer to [Mobius\examples](./examples) directory or [Mobius\csharp\Samples](./csharp/Samples) directory.

## API Documentation

Refer to [Mobius C# API documentation](./csharp/Adapter/documentation/Mobius_API_Documentation.md) for the list of Spark's data processing operations supported in Mobius.

## API Usage

Mobius API usage samples are available at:

* [Examples folder](./examples) which contains standalone [C# and F# projects](./notes/running-mobius-app.md#running-mobius-examples-in-local-mode) that can be used as templates to start developing Mobius applications

* [Samples project](./csharp/Samples/Microsoft.Spark.CSharp/) which uses a comprehensive set of Mobius APIs to implement samples that are also used for functional validation of APIs

* Mobius performance test scenarios implemented in [C#](./csharp/Perf/Microsoft.Spark.CSharp) and [Scala](./scala/perf) for side by side comparison of Spark driver code

## Documents

Refer to the [docs folder](docs) for design overview and other info on Mobius

## Build Status

|Ubuntu 14.04.3 LTS |Windows |Unit test coverage |
|-------------------|:------:|:-----------------:|
|[![Build status](https://travis-ci.org/Microsoft/Mobius.svg?branch=master)](https://travis-ci.org/Microsoft/Mobius) |[![Build status](https://ci.appveyor.com/api/projects/status/lflkua81gg0swv6i/branch/master?svg=true)](https://ci.appveyor.com/project/SparkCLR/sparkclr/branch/master) |[![codecov.io](https://codecov.io/github/Microsoft/Mobius/coverage.svg?branch=master)](https://codecov.io/github/Microsoft/Mobius?branch=master)

## Getting Started

| |Windows |Linux |
|---|:------|:----|
|Build & run unit tests |[Build in Windows](notes/windows-instructions.md#building-mobius) |[Build in Linux](notes/linux-instructions.md#building-mobius-in-linux) |
|Run samples (functional tests) in local mode |[Samples in Windows](notes/windows-instructions.md#running-samples) |[Samples in Linux](notes/linux-instructions.md#running-mobius-samples-in-linux) |
|Run examples in local mode |[Examples in Windows](/notes/running-mobius-app.md#running-mobius-examples-in-local-mode) |[Examples in Linux](notes/linux-instructions.md#running-mobius-examples-in-linux) |
|Run Mobius app |<ul><li>[Standalone cluster](notes/running-mobius-app.md#standalone-cluster)</li><li>[YARN cluster](notes/running-mobius-app.md#yarn-cluster)</li></ul> |<ul><li>[Linux cluster](notes/linux-instructions.md#running-mobius-applications-in-linux)</li><li>[Azure HDInsight Spark Cluster](/notes/mobius-in-hdinsight.md)</li><li>[AWS EMR Spark Cluster](/notes/linux-instructions.md#mobius-in-amazon-web-services-emr-spark-cluster)</li> |
|Run Mobius Shell |<ul><li>[Local](notes/mobius-shell.md#run-shell)</li><li>[YARN](notes/mobius-shell.md#run-shell)</li></ul> | Not supported yet |

### Useful Links
* [Configuration parameters in Mobius](./notes/configuration-mobius.md)
* [Troubleshoot errors in Mobius](./notes/troubleshooting-mobius.md)
* [Debug Mobius apps](./notes/running-mobius-app.md#debug-mode)
* [Implementing Spark Apps in F# using Mobius](./notes/spark-fsharp-mobius.md)

## Supported Spark Versions

Mobius is built and tested with Apache Spark [1.4.1](https://github.com/Microsoft/Mobius/tree/branch-1.4), [1.5.2](https://github.com/Microsoft/Mobius/tree/branch-1.5), [1.6.*](https://github.com/Microsoft/Mobius/tree/branch-1.6) and [2.0](https://github.com/Microsoft/Mobius/tree/branch-2.0).

## Releases

Mobius releases are available at https://github.com/Microsoft/Mobius/releases. References needed to build C# Spark driver application using Mobius are also available in [NuGet](https://www.nuget.org/packages/Microsoft.SparkCLR)

[![NuGet Badge](https://buildstats.info/nuget/Microsoft.SparkCLR)](https://www.nuget.org/packages/Microsoft.SparkCLR)

Refer to [mobius-release-info.md](./notes/mobius-release-info.md) for the details on versioning policy and the contents of the release.

## License

[![License](https://img.shields.io/badge/license-MIT-blue.svg?style=plastic)](https://github.com/Microsoft/Mobius/blob/master/LICENSE)

Mobius is licensed under the MIT license. See [LICENSE](LICENSE) file for full license information.


## Community

[![Issue Stats](http://issuestats.com/github/Microsoft/Mobius/badge/pr)](http://issuestats.com/github/Microsoft/Mobius)
[![Issue Stats](http://issuestats.com/github/Microsoft/Mobius/badge/issue)](http://issuestats.com/github/Microsoft/Mobius)
[![Join the chat at https://gitter.im/Microsoft/Mobius](https://badges.gitter.im/Microsoft/Mobius.svg)](https://gitter.im/Microsoft/Mobius?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Twitter](https://img.shields.io/twitter/url/http/twitter.com/MobiusForSpark.svg?style=social)](https://twitter.com/intent/tweet?text=%40MobiusForSpark%20%5Byour%20tweet%5D%20via%20%40GitHub)

* Mobius project welcomes contributions. To contribute, follow the instructions in [CONTRIBUTING.md](./notes/CONTRIBUTING.md)

* Options to ask your question to the Mobius community
  * create issue on [GitHub](https://github.com/Microsoft/Mobius)
  * create post with "sparkclr" tag in [Stack Overflow](https://stackoverflow.com/questions/tagged/sparkclr)
  * join chat at [Mobius room in Gitter](https://gitter.im/Microsoft/Mobius)
  * tweet [@MobiusForSpark](http://twitter.com/MobiusForSpark)

## Code of Conduct
This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
