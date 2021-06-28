using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLiteDump.Job.SQLiteAccess
{
    public class TableSchema
    {
        public string TableName;

        public string TableSchemaName;

        public List<ColumnSchema> Columns;

        public List<string> PrimaryKey;

        public List<ForeignKeySchema> ForeignKeys;

        public List<IndexSchema> Indexes;
        
        public string Table
        {
            get
            {
                return TableSchemaName + "." + TableName;
            }
        }
    }
}
