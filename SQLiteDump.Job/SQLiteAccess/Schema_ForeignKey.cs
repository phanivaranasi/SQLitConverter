using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLiteDump.Job.SQLiteAccess
{
	public class ForeignKeySchema
	{
		public string TableName;

		public string ColumnName;

		public string ForeignTableName;

		public string ForeignColumnName;

		public bool CascadeOnDelete;

		public bool IsNullable;
	}
}
