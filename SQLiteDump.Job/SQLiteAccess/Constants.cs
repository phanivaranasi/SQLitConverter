using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLiteDump.Job.SQLiteAccess
{
    public static class SQLQuery
    {
        public static string GetSysDatabases
        {
            get
            {
                return "SELECT DISTINCT [NAME] FROM SYSDATABASES";
            }
        }
        public static string GetDBSchemas
        {
            get
            {
                return "SELECT TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES  WHERE TABLE_TYPE = 'BASE TABLE' Order by TABLE_SCHEMA";
            }
        }
        public static string GetTableCols
        {
            get
            {
                return "SELECT COLUMN_NAME,COLUMN_DEFAULT,IS_NULLABLE,DATA_TYPE, " +
                       " (columnproperty(object_id(TABLE_NAME), COLUMN_NAME, 'IsIdentity')) AS[IDENT], " +
                       " CHARACTER_MAXIMUM_LENGTH AS CSIZE " +
                       " FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{0}' AND TABLE_SCHEMA='{1}' " +
                       " ORDER BY ORDINAL_POSITION ASC";
            }
        }
        public static string GetTableForeignKeys
        {
            get
            {
                return @"SELECT " +
                @"  ColumnName = CU.COLUMN_NAME, " +
                @"  ForeignTableName  = PK.TABLE_NAME, " +
                @"  ForeignColumnName = PT.COLUMN_NAME, " +
                @"  DeleteRule = C.DELETE_RULE, " +
                @"  IsNullable = COL.IS_NULLABLE " +
                @"FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS C " +
                @"INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS FK ON C.CONSTRAINT_NAME = FK.CONSTRAINT_NAME " +
                @"INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS PK ON C.UNIQUE_CONSTRAINT_NAME = PK.CONSTRAINT_NAME " +
                @"INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE CU ON C.CONSTRAINT_NAME = CU.CONSTRAINT_NAME " +
                @"INNER JOIN " +
                @"  ( " +
                @"    SELECT i1.TABLE_NAME, i2.COLUMN_NAME " +
                @"    FROM  INFORMATION_SCHEMA.TABLE_CONSTRAINTS i1 " +
                @"    INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE i2 ON i1.CONSTRAINT_NAME = i2.CONSTRAINT_NAME " +
                @"    WHERE i1.CONSTRAINT_TYPE = 'PRIMARY KEY' " +
                @"  ) " +
                @"PT ON PT.TABLE_NAME = PK.TABLE_NAME " +
                @"INNER JOIN INFORMATION_SCHEMA.COLUMNS AS COL ON CU.COLUMN_NAME = COL.COLUMN_NAME AND FK.TABLE_NAME = COL.TABLE_NAME " +
                @"WHERE FK.Table_NAME='{0}'";
            }
        }
        public static string GetTableKeys
        {
            get
            {
                return "EXEC SP_PKEYS '{0}'";
            }
        }

        public static string GetTableCollateInfo
        {
            get
            {
                return "EXEC SP_TABLECOLLATIONS '{0}'";
            }
        }

        public static string GetTableIndexInfo
        {
            get
            {
                return "EXEC SP_HELPINDEX '{0}'";
            }
        }

        public static string GetSQLViews
        {
            get
            {
                return "SELECT TABLE_NAME, VIEW_DEFINITION  from INFORMATION_SCHEMA.VIEWS";
            }
        }
    }
}
