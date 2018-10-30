﻿using System.Runtime.Serialization;
using Microsoft.Spark.CSharp.Core;
using System.Collections.Generic;

namespace Microsoft.Spark.CSharp
{
    internal class WorkerFunc
    {
        private int stageId;
        private CSharpWorkerFunc func;
        private int argsCount;
        private List<int> argOffsets;

        public WorkerFunc(CSharpWorkerFunc func, int argsCount, List<int> argOffsets, int stageId)
        {
            this.func = func;
            this.argsCount = argsCount;
            this.argOffsets = argOffsets;
            this.stageId = stageId;
        }

        public int StageId
        {
            get
            {
                return stageId;
            }

            set
            {
                stageId = value;
            }
        }

        internal CSharpWorkerFunc Func
        {
            get
            {
                return func;
            }

            set
            {
                func = value;
            }
        }

        public int ArgsCount
        {
            get
            {
                return argsCount;
            }

            set
            {
                argsCount = value;
            }
        }

        public List<int> ArgOffsets
        {
            get
            {
                return argOffsets;
            }

            set
            {
                argOffsets = value;
            }
        }
    }
}
