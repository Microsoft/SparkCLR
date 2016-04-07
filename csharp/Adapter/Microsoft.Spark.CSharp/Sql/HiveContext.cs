﻿using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Proxy;

namespace Microsoft.Spark.CSharp.Sql
{
    /// <summary>
    /// A variant of Spark SQL that integrates with data stored in Hive. 
    /// Configuration for Hive is read from hive-site.xml on the classpath.
    /// It supports running both SQL and HiveQL commands.
    /// </summary>
    public class HiveContext : SqlContext
    {
        /// <summary>
        /// Creates a HiveContext
        /// </summary>
        /// <param name="sparkContext"></param>
        public HiveContext(SparkContext sparkContext) 
            : base(sparkContext, sparkContext.SparkContextProxy.CreateHiveContext())
        {
        }

        internal HiveContext(SparkContext sparkContext, ISqlContextProxy sqlContextProxy)
            : base(sparkContext, sqlContextProxy)
        {
        }

        /// <summary>
        /// Invalidate and refresh all the cached the metadata of the given table.
        /// For performance reasons, Spark SQL or the external data source library it uses
        /// might cache certain metadata about a table, such as the location of blocks.
        /// When those change outside of Spark SQL, users should call this function to invalidate the cache.
        /// </summary>
        /// <param name="tableName"></param>
        public void RefreshTable(string tableName)
        {
            SqlContextProxy.RefreshTable(tableName);
        }
    }
}
