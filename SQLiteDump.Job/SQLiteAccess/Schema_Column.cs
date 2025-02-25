﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLiteDump.Job.SQLiteAccess
{
    public class ColumnSchema
    {
        public string ColumnName;

        public string ColumnType;

        public int Length;

        public bool IsNullable;

        public string DefaultValue;

        public bool IsIdentity;

        public bool? IsCaseSensitivite = null;
    }
}
