﻿// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Sql;

namespace Microsoft.Spark.CSharp.Examples
{
    /// <summary>
    /// This example shows how to use Cassandra in .NET Apache Spark DataFrame API
    /// It covers load rows from Cassandra, filter it and save results to another table
    /// </summary>
    public class CassandraDataFrameExample
    {
        static void Main(string[] args)
        {
            var cassandraHostName = "localhost";
            var cassandraKeySpace = "ks";
            var cassandraTable = "users";

            /*
                ** CQL used to create data in Cassandra for this example **
                
                CREATE TABLE users (
	                username VARCHAR,
	                firstname VARCHAR,
	                lastname VARCHAR,
	            PRIMARY KEY (username)
                );

                INSERT INTO ks.users (username, firstname, lastname) VALUES ('JD123', 'John', 'Doe');
                INSERT INTO ks.users (username, firstname, lastname) VALUES ('BillJ', 'Bill', 'Jones');
                INSERT INTO ks.users (username, firstname, lastname) VALUES ('SL', 'Steve', 'Little');
             */

            var sparkConf = new SparkConf().Set("spark.cassandra.connection.host", cassandraHostName);
            var sparkContext = new SparkContext(sparkConf);
            var sqlContext = new SqlContext(sparkContext);

            //read from cassandra table
            var usersDataFrame =
                sqlContext.Read()
                    .Format("org.apache.spark.sql.cassandra")
                    .Options(new Dictionary<string, string> { {"keyspace", cassandraKeySpace }, { "table", cassandraTable } })
                    .Load();

            //display rows in the console
            usersDataFrame.Show();

            var createTempTableStatement =
                string.Format(
                    "CREATE TEMPORARY TABLE userstemp USING org.apache.spark.sql.cassandra OPTIONS(table \"{0}\", keyspace \"{1}\")", 
                    cassandraTable, 
                    cassandraKeySpace);

            //create a temp table
            sqlContext.Sql(createTempTableStatement);

            //read from temp table, filter it and display schema and rows
            var filteredUsersDataFrame = sqlContext.Sql("SELECT * FROM userstemp").Filter("username = 'SL'");
            filteredUsersDataFrame.ShowSchema();
            filteredUsersDataFrame.Show();

            //write filtered rows to another table
            filteredUsersDataFrame.Write()
                .Format("org.apache.spark.sql.cassandra")
                .Options(new Dictionary<string, string> { { "keyspace", cassandraKeySpace }, { "table", "filteredusers" } })
                .Save();

            //convert to RDD, execute map & filter and collect result
            var rddCollectedItems = usersDataFrame.ToRDD()
                                    .Map(
                                        r =>
                                            string.Format("{0},{1},{2}", r.GetAs<string>("username"), 
                                                                         r.GetAs<string>("firstname"),
                                                                         r.GetAs<string>("lastname")))
                                    .Filter(s => s.Contains("SL"))
                                    .Collect();

            foreach (var rddCollectedItem in rddCollectedItems)
            {
                Console.WriteLine(rddCollectedItem);
            }

            Console.WriteLine("Completed running example");
        }
    }
}
