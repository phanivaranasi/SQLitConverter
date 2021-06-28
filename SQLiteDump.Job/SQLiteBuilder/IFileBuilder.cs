using SQLiteDump.Job.SQLiteAccess;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLiteDump.Job.SQLiteBuilder
{
    public interface IFileBuilder
    {
        void HandleFile();
        TableSchema ReadSqlServerSchema(string tableName, string schema, SqlConnection con);
        List<ViewSchema> ReadSqlServerViews(SqlConnection con);
        string StripParens(string value);
        string DiscardNational(string value);
        bool IsValidDefaultValue(string value);
    }
}
