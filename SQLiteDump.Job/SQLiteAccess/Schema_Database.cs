using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLiteDump.Job.SQLiteAccess
{
    public class DatabaseSchema
    {
        public List<TableSchema> Tables = new List<TableSchema>();
        public List<ViewSchema> Views = new List<ViewSchema>();
    }
}
