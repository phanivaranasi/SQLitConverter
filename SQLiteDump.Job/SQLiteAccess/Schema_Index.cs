using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLiteDump.Job.SQLiteAccess
{
    public class IndexSchema
    {
        public string IndexName;

        public bool IsUnique;

        public List<IndexColumn> Columns;


       

    }

    public class IndexColumn
    {
        public string ColumnName;
        public bool IsAscending;
    }
}
