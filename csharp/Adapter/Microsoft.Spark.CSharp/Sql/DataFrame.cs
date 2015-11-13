﻿// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;
using System.Net.Sockets;

using Razorvine.Pickle;

using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Interop;

namespace Microsoft.Spark.CSharp.Sql
{
    /// <summary>
    ///  A distributed collection of data organized into named columns.
    /// 
    /// See also http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrame
    /// </summary>
    public class DataFrame
    {
        private readonly IDataFrameProxy dataFrameProxy;
        private readonly SparkContext sparkContext;
        private StructType schema;
        private RowSchema rowSchema;
        
        internal SparkContext SparkContext
        {
            get
            {
                return sparkContext;
            }
        }

        internal IDataFrameProxy DataFrameProxy
        {
            get { return dataFrameProxy;  }
        }

        public StructType Schema
        {
            get { return schema ?? (schema = new StructType(dataFrameProxy.GetSchema())); }
        }

        public Column this[string columnName]
        {
            get
            {
                return new Column(dataFrameProxy.GetColumn(columnName));
            }
        }

        internal DataFrame(IDataFrameProxy dataFrameProxy, SparkContext sparkContext)
        {
            this.dataFrameProxy = dataFrameProxy;
            this.sparkContext = sparkContext;
        }

        /// <summary>
        /// Registers this DataFrame as a temporary table using the given name.  The lifetime of this 
        /// temporary table is tied to the SqlContext that was used to create this DataFrame.
        /// </summary>
        /// <param name="tableName">Name of the table</param>
        public void RegisterTempTable(string tableName)
        {
            dataFrameProxy.RegisterTempTable(tableName);
        }

        /// <summary>
        /// Number of rows in the DataFrame
        /// </summary>
        /// <returns>row count</returns>
        public long Count()
        {
            return dataFrameProxy.Count();
        }

        /// <summary>
        /// Displays rows of the DataFrame in tabular form
        /// </summary>
        /// <param name="numberOfRows">Number of rows to display - default 20</param>
        /// <param name="truncate">Indicates if strings more than 20 characters long will be truncated</param>
        public void Show(int numberOfRows = 20, bool truncate = true)
        {
            Console.WriteLine(dataFrameProxy.GetShowString(numberOfRows, truncate));
        }

        /// <summary>
        /// Prints the schema information of the DataFrame
        /// </summary>
        public void ShowSchema()
        {
            List<string> nameTypeList = Schema.Fields.Select(structField => string.Format("{0}:{1}", structField.Name, structField.DataType.SimpleString())).ToList();
            Console.WriteLine(string.Join(", ", nameTypeList));
        }

        /// <summary>
        /// Returns all of Rows in this DataFrame
        /// </summary>
        public IEnumerable<Row> Collect()
        {
            int port = dataFrameProxy.CollectAndServe();
            RowSchema rs = GetRowSchema();
            List<Row> items = Collect(port, rs);
            return items.AsEnumerable<Row>();
        }

        private RowSchema GetRowSchema()
        {
            if (rowSchema == null)
            {
                string json = Schema.ToJson();
                rowSchema = RowSchema.ParseRowSchemaFromJson(json);
            }
            return rowSchema;
        }

        private List<Row> Collect(int port, RowSchema dataType)
        {
            List<Row> items = new List<Row>();
            IFormatter formatter = new BinaryFormatter();
            Unpickler unpickler = new Unpickler();
            Socket sock = new Socket(SocketType.Stream, ProtocolType.Tcp);
            sock.Connect("127.0.0.1", port);
            NetworkStream ns = new NetworkStream(sock);

            using (BinaryReader br = new BinaryReader(ns))
            {
                byte[] buffer = null;
                do
                {
                    buffer = br.ReadBytes(4);
                    if (buffer != null && buffer.Length > 0)
                    {
                        //In JVM, Multibyte data items are always stored in big-endian order, where the high bytes come first
                        //http://docs.oracle.com/javase/specs/jvms/se7/html/jvms-4.html
                        //So we should handle revert buffer if our system is little-endian
                        //https://msdn.microsoft.com/en-us/library/system.bitconverter(v=vs.110).aspx
                        if (BitConverter.IsLittleEndian)
                        {
                            Array.Reverse(buffer);
                        }

                        int len = BitConverter.ToInt32(buffer, 0);
                        byte[] data = br.ReadBytes(len);
                        foreach (var item in (unpickler.loads(data) as object[]))
                        {
                            RowImpl row = new RowImpl(item, dataType);
                            items.Add(row);
                        }
                    }
                } while (buffer != null && buffer.Length > 0);
            }

            return items;
        }

        /// <summary>
        /// Converts the DataFrame to RDD of byte[]
        /// </summary>
        /// <returns>resulting RDD</returns>
        public RDD<byte[]> ToRDD() //RDD created using byte representation of GenericRow objects
        {
            return new RDD<byte[]>(dataFrameProxy.ToRDD(), sparkContext);
        }

        /// <summary>
        /// Returns the content of the DataFrame as RDD of JSON strings
        /// </summary>
        /// <returns>resulting RDD</returns>
        public RDD<string> ToJSON()
        {
            var stringRddReference = dataFrameProxy.ToJSON();
            return new RDD<string>(stringRddReference, sparkContext);
        }

        /// <summary>
        /// Prints the plans (logical and physical) to the console for debugging purposes
        /// </summary>
        /// <param name="extended">if true prints both query plan and execution plan; otherwise just prints query plan</param>
        public void Explain(bool extended = false) //TODO - GetQueryExecution is called in JVM twice if extendd = true - fix that
        {
            Console.WriteLine(dataFrameProxy.GetQueryExecution());
            if (extended)
            {
                Console.WriteLine(dataFrameProxy.GetExecutedPlan());
            }
        }

        /// <summary>
        /// Selects a set of columns. This is a variant of `select` that can only select
        /// existing columns using column names (i.e. cannot construct expressions).
        /// 
        /// df.Select("colA", "colB")
        /// 
        /// </summary>
        /// <param name="firstColumnName">first column name - required</param>
        /// <param name="otherColumnNames">other column names - optional</param>
        /// <returns></returns>
        public DataFrame Select(string firstColumnName, params string[] otherColumnNames)
        {
            return new DataFrame(dataFrameProxy.Select(firstColumnName, otherColumnNames), sparkContext);
        }

        /// <summary>
        /// Selects a set of SQL expressions. This is a variant of `select` that accepts SQL expressions.
        ///
        ///   df.SelectExpr("colA", "colB as newName", "abs(colC)")
        ///   
        /// </summary>
        /// <param name="columnExpressions"></param>
        /// <returns></returns>
        public DataFrame SelectExpr(params string[] columnExpressions)
        {
            return new DataFrame(dataFrameProxy.SelectExpr(columnExpressions), sparkContext);
        }

        // /// <summary>
        // /// TO DO:  to be decided whether to expose this API
        // /// 
        // ///     1. has alternative - sql("<SQL scripts>")
        // ///     2. perf impact comapred to sql() - 1 more java call per each Column in select list
        // ///     
        // /// Select a list of columns
        // /// </summary>
        // /// <param name="columns"></param>
        // /// <returns></returns>
        //public DataFrame Select(params Column[] columns)
        //{
        //    List<IColumnProxy> columnReferenceList = columns.Select(column => column.ColumnProxy).ToList();
        //    IColumnProxy columnReferenceSeq = dataFrameProxy.ToColumnSeq(columnReferenceList);
        //    return new DataFrame(dataFrameProxy.Select(columnReferenceSeq), sparkContext);
        //}

        /// <summary>
        /// Filters rows using the given condition
        /// </summary>
        /// <param name="condition"></param>
        /// <returns></returns>
        public DataFrame Where(string condition)
        {
            return Filter(condition);
        }

        /// <summary>
        /// Filters rows using the given condition
        /// </summary>
        /// <param name="condition"></param>
        /// <returns></returns>
        public DataFrame Filter(string condition)
        {
            return new DataFrame(dataFrameProxy.Filter(condition), sparkContext);
        }

        /// <summary>
        /// Groups the DataFrame using the specified columns, so we can run aggregation on them.
        /// </summary>
        /// <param name="firstColumnName">first column name - required</param>
        /// <param name="otherColumnNames">other column names - optional</param>
        /// <returns></returns>
        public GroupedData GroupBy(string firstColumnName, params string[] otherColumnNames)
        {
            var scalaGroupedDataReference = dataFrameProxy.GroupBy(firstColumnName, otherColumnNames);
            return new GroupedData(scalaGroupedDataReference, this);
        }

        private GroupedData GroupBy()
        {
            var scalaGroupedDataReference = dataFrameProxy.GroupBy();
            return new GroupedData(scalaGroupedDataReference, this);
        }

        /// <summary>
        /// Aggregates on the DataFrame for the given column-aggregate function mapping
        /// </summary>
        /// <param name="columnNameAggFunctionDictionary"></param>
        /// <returns></returns>
        public DataFrame Agg(Dictionary<string, string> columnNameAggFunctionDictionary)
        {
            return GroupBy().Agg(columnNameAggFunctionDictionary);
        }

        /// <summary>
        /// Join with another DataFrame - Cartesian join
        /// </summary>
        /// <param name="otherDataFrame">DataFrame to join with</param>
        /// <returns>Joined DataFrame</returns>
        public DataFrame Join(DataFrame otherDataFrame)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Join with another DataFrame - Inner equi-join using given column name
        /// </summary>
        /// <param name="otherDataFrame">DataFrame to join with</param>
        /// <param name="joinColumnName">Column to join with.</param>
        /// <returns>Joined DataFrame</returns>
        public DataFrame Join(DataFrame otherDataFrame, string joinColumnName) // TODO: need aliasing for self join
        {
            return new DataFrame(
                dataFrameProxy.Join(otherDataFrame.dataFrameProxy, joinColumnName),
                sparkContext);
        }

        /// <summary>
        /// Join with another DataFrame - Inner equi-join using given column name 
        /// </summary>
        /// <param name="otherDataFrame">DataFrame to join with</param>
        /// <param name="joinColumnNames">Columns to join with.</param>
        /// <returns>Joined DataFrame</returns>
        public DataFrame Join(DataFrame otherDataFrame, string[] joinColumnNames) // TODO: need aliasing for self join
        {
            return new DataFrame(
                dataFrameProxy.Join(otherDataFrame.dataFrameProxy, joinColumnNames),
                sparkContext);
        }

        /// <summary>
        /// Join with another DataFrame, using the specified JoinType
        /// </summary>
        /// <param name="otherDataFrame">DataFrame to join with</param>
        /// <param name="joinExpression">Column to join with.</param>
        /// <param name="joinType">Type of join to perform (default null value means <c>JoinType.Inner</c>)</param>
        /// <returns>Joined DataFrame</returns>
        public DataFrame Join(DataFrame otherDataFrame, Column joinExpression, JoinType joinType = null) 
        {
            if (joinType == null)
            {
                joinType = JoinType.Inner;
            }

            return
                new DataFrame(dataFrameProxy.Join(otherDataFrame.dataFrameProxy, joinExpression.ColumnProxy, joinType.Value), sparkContext);
        }

        /// <summary>
        /// Intersect with another DataFrame.
        /// This is equivalent to `INTERSECT` in SQL.
        /// </summary>
        /// <param name="otherDataFrame">DataFrame to intersect with.</param>
        /// <returns>Intersected DataFrame.</returns>
        public DataFrame Intersect(DataFrame otherDataFrame)
        {
            return
                new DataFrame(dataFrameProxy.Intersect(otherDataFrame.dataFrameProxy), sparkContext);
        }

        /// <summary>
        /// Union with another DataFrame WITHOUT removing duplicated rows.
        /// This is equivalent to `UNION ALL` in SQL.
        /// </summary>
        /// <param name="otherDataFrame">DataFrame to union all with.</param>
        /// <returns>Unioned DataFrame.</returns>
        public DataFrame UnionAll(DataFrame otherDataFrame)
        {
            return
                new DataFrame(dataFrameProxy.UnionAll(otherDataFrame.dataFrameProxy), sparkContext);
        }

        /// <summary>
        /// Returns a new DataFrame containing rows in this frame but not in another frame.
        /// This is equivalent to `EXCEPT` in SQL.
        /// </summary>
        /// <param name="otherDataFrame">DataFrame to subtract from this frame.</param>
        /// <returns>A new DataFrame containing rows in this frame but not in another frame.</returns>
        public DataFrame Subtract(DataFrame otherDataFrame)
        {
            return
                new DataFrame(dataFrameProxy.Subtract(otherDataFrame.dataFrameProxy), sparkContext);
        }

        /// <summary>
        /// Returns a new DataFrame with a column dropped.
        /// </summary>
        /// <param name="columnName"> a string name of the column to drop</param>
        /// <returns>A new new DataFrame that drops the specified column.</returns>
        public DataFrame Drop(string columnName)
        {
            return
                new DataFrame(dataFrameProxy.Drop(columnName), sparkContext);
        }

        /// <summary>
        /// Returns a new DataFrame omitting rows with null values.
        /// </summary>
        /// <param name="how">'any' or 'all'. 
        /// If 'any', drop a row if it contains any nulls.
        /// If 'all', drop a row only if all its values are null.</param>
        /// <param name="thresh">thresh: int, default null.
        /// If specified, drop rows that have less than `thresh` non-null values.
        /// This overwrites the `how` parameter.</param>
        /// <param name="subset">optional list of column names to consider.</param>
        /// <returns>A new DataFrame omitting rows with null values</returns>
        public DataFrame DropNa(string how = "any", int? thresh = null, string[] subset = null)
        {
            return
                new DataFrame(dataFrameProxy.DropNa(how, thresh, subset), sparkContext);
        }

        /// <summary>
        /// Returns a new DataFrame with duplicate rows removed, considering only the subset of columns.
        /// </summary>
        /// <param name="subset">drop duplicated rows on these columns.</param>
        /// <returns>A new DataFrame with duplicate rows removed.</returns>
        public DataFrame DropDuplicates(string[] subset = null)
        {
            return 
                new DataFrame(dataFrameProxy.DropDuplicates(subset), sparkContext);
        }

        /// <summary>
        /// Returns a new DataFrame replacing a value with another value.
        /// </summary>
        /// <typeparam name="T">Data type of value to replace.</typeparam>
        /// <param name="toReplace">Value to be replaced. The value to be replaced must be an int, long, float, or string and must be the same type as <paramref name="value"/>.</param>
        /// <param name="value">Value to use to replace holes. The replacement value must be an int, long, float, or string and must be the same type as <paramref name="toReplace"/>.</param>
        /// <param name="subset">Optional list of column names to consider.</param>
        /// <returns>A new DataFrame replacing a value with another value</returns>
        public DataFrame Replace<T>(T toReplace, T value, string[] subset = null)
        {
            return new DataFrame(dataFrameProxy.Replace(toReplace, value, subset), sparkContext);
        }

        /// <summary>
        /// Returns a new DataFrame replacing values with other values.
        /// </summary>
        /// <typeparam name="T">Data type of values to replace.</typeparam>
        /// <param name="toReplace">List of values to be replaced. The value to be replaced must be an int, long, float, or string and must be the same type as <paramref name="value"/>. 
        /// This list should be of the same length with <paramref name="value"/>.</param>
        /// <param name="value">List of values to replace holes. The replacement must be an int, long, float, or string and must be the same type as <paramref name="toReplace"/>.
        /// This list should be of the same length with <paramref name="toReplace"/>.</param>
        /// <param name="subset">Optional list of column names to consider.</param>
        /// <returns>A new DataFrame replacing values with other values</returns>
        public DataFrame ReplaceAll<T>(IEnumerable<T> toReplace, IEnumerable<T> value, string[] subset = null)
        {
            return new DataFrame(dataFrameProxy.ReplaceAll(toReplace, value, subset), sparkContext);
        }

        /// <summary>
        /// Returns a new DataFrame replacing values with another value.
        /// </summary>
        /// <typeparam name="T">Data type of values to replace.</typeparam>
        /// <param name="toReplace">List of values to be replaced. The value to be replaced must be an int, long, float, or string and must be the same type as <paramref name="value"/>.</param>
        /// <param name="value">Value to use to replace holes. The replacement value must be an int, long, float, or string and must be the same type as <paramref name="toReplace"/>.</param>
        /// <param name="subset">Optional list of column names to consider.</param>
        /// <returns>A new DataFrame replacing values with another value</returns>
        public DataFrame ReplaceAll<T>(IEnumerable<T> toReplace, T value, string[] subset = null)
        {
            return new DataFrame(dataFrameProxy.ReplaceAll(toReplace, value, subset), sparkContext);
        }

        /// <summary>
        /// Returns a new DataFrame by taking the first `n` rows.
        /// The difference between this function and `head` is that `head` returns an array while `limit` returns a new DataFrame.
        /// </summary>
        /// <param name="num">Number of rows to take from current DataFrame</param>
        /// <returns>A new DataFrame containing the first `n` rows</returns>
        public DataFrame Limit(int num)
        {
            return
                new DataFrame(dataFrameProxy.Limit(num), sparkContext);
        }
    }

    public class JoinType
    {
        public string Value { get; private set; }
        private JoinType(string value)
        {
            Value = value;
        }

        private static readonly JoinType InnerJoinType = new JoinType("inner");
        private static readonly JoinType OuterJoinType = new JoinType("outer");
        private static readonly JoinType LeftOuterJoinType = new JoinType("left_outer");
        private static readonly JoinType RightOuterJoinType = new JoinType("right_outer");
        private static readonly JoinType LeftSemiJoinType = new JoinType("leftsemi");

        public static JoinType Inner
        {
            get
            {
                return InnerJoinType;
            }
        }

        public static JoinType Outer
        {
            get
            {
                return OuterJoinType;
            }
        }

        public static JoinType LeftOuter
        {
            get
            {
                return LeftOuterJoinType;
            }
        }

        public static JoinType RightOuter
        {
            get
            {
                return RightOuterJoinType;
            }
        }

        public static JoinType LeftSemi
        {
            get
            {
                return LeftSemiJoinType;
            }
        }
    }

    public class GroupedData
    {
        private IGroupedDataProxy groupedDataProxy;
        private DataFrame dataFrame;

        internal GroupedData(IGroupedDataProxy groupedDataProxy, DataFrame dataFrame)
        {
            this.groupedDataProxy = groupedDataProxy;
            this.dataFrame = dataFrame;
        }

        public DataFrame Agg(Dictionary<string, string> columnNameAggFunctionDictionary)
        {
            return new DataFrame(dataFrame.DataFrameProxy.Agg(groupedDataProxy, columnNameAggFunctionDictionary), dataFrame.SparkContext);
        }
    }
}
