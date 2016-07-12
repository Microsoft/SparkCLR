﻿// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Spark.CSharp.Core;

namespace Microsoft.Spark.CSharp.Proxy
{
    internal interface IStreamingContextProxy
    {
        SparkContext SparkContext { get; }
        void Start();
        void Stop();
        void Remember(int durationSeconds);
        void Checkpoint(string directory);
        IDStreamProxy TextFileStream(string directory);
        IDStreamProxy SocketTextStream(string hostname, int port, StorageLevelType storageLevelType);
        IDStreamProxy KafkaStream(Dictionary<string, int> topics, Dictionary<string, string> kafkaParams, StorageLevelType storageLevelType);
        IDStreamProxy DirectKafkaStream(List<string> topics, Dictionary<string, string> kafkaParams, Dictionary<string, long> fromOffsets);
        IDStreamProxy DirectKafkaStreamWithRepartition(List<string> topics, Dictionary<string, string> kafkaParams, Dictionary<string, long> fromOffsets,
            int numPartitions, byte[] readFunc, string serializationMode);
        IDStreamProxy Union(IDStreamProxy firstDStreams, IDStreamProxy[] otherDStreams);
        void AwaitTermination();
        void AwaitTerminationOrTimeout(long timeout);
        IDStreamProxy CreateCSharpDStream(IDStreamProxy jdstream, byte[] func, string serializationMode);
        IDStreamProxy CreateCSharpTransformed2DStream(IDStreamProxy jdstream, IDStreamProxy jother, byte[] func, string serializationMode, string serializationModeOther);
        IDStreamProxy CreateCSharpReducedWindowedDStream(IDStreamProxy jdstream, byte[] func, byte[] invFunc, int windowSeconds, int slideSeconds, string serializationMode);
        IDStreamProxy CreateCSharpStateDStream(IDStreamProxy jdstream, byte[] func, string className, string serializationMode, string serializationMode2);
        IDStreamProxy CreateConstantInputDStream(IRDDProxy rddProxy);
        IDStreamProxy CreateCSharpInputDStream(byte[] func, string serializationMode);
        IDStreamProxy EventHubsUnionStream(Dictionary<string, string> eventHubsParams, StorageLevelType storageLevelType);

    }
}
