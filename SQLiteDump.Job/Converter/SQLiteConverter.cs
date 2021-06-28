using Microsoft.Extensions.Configuration;
using Serilog;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using SQLiteDump.Job.SQLiteAccess;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SQLiteDump.Job.SQLiteBuilder;
using System.Data.SQLite;
using System.Data;

namespace SQLiteDump.Job
{
    public class SQLiteConverter : ISQLiteConverter
    {
        IConfiguration Configuration;
        ILogger Logger;
        IFileBuilder FileBuilder;
        IList<string> sqlDatabases = new List<string>();
        IList<string> tableNames = new List<string>();
        IList<string> tblschema = new List<string>();
        IList<TableSchema> tbls = new List<TableSchema>();
        string SQLiteFilePath;
        public SQLiteConverter(IConfiguration configuration, ILogger logger, IFileBuilder fileBuilder)
        {
            Configuration = configuration;
            Logger = logger;
            FileBuilder = fileBuilder;
            Logger.Information("SQLite Conversion Initated");
        }

        public void IntiConversion()
        {
            string sqlConnStr = Configuration["SQLConnectionString"].ToString();
            Logger.Information($"SQL Connection String : {sqlConnStr}");
            DatabaseSchema ds = GetSQLDatabases(sqlConnStr);
            GenerateSQLite(ds);
            string sqliteConnStr = CreateSQLiteConnectionString();
            CopyTuples(sqlConnStr, sqliteConnStr, ref ds);

        }


        private DatabaseSchema GetSQLDatabases(string sqlConnStr)
        {
            DatabaseSchema ds = new DatabaseSchema();
            try
            {
                using (SqlConnection con = new SqlConnection(sqlConnStr))
                {
                    con.Open();
                    Logger.Information($"Started Reading SQLServer Information {con.State}");
                    SqlCommand cmd = new SqlCommand(SQLQuery.GetSysDatabases, con);

                    //Fetching databases
                    using (SqlDataReader sqlDr = cmd.ExecuteReader())
                    {
                        while (sqlDr.Read())
                        {
                            sqlDatabases.Add(sqlDr[0].ToString());
                        }
                    }

                    //
                    using (cmd = new SqlCommand(SQLQuery.GetDBSchemas, con))
                    {
                        using (SqlDataReader sqlDr = cmd.ExecuteReader())
                        {
                            while (sqlDr.Read())
                            {
                                if (sqlDr["TABLE_NAME"] == DBNull.Value)
                                    continue;
                                if (sqlDr["TABLE_SCHEMA"] == DBNull.Value)
                                    continue;
                                tableNames.Add((string)sqlDr["TABLE_NAME"]);
                                tblschema.Add((string)sqlDr["TABLE_SCHEMA"]);
                            }
                        }
                    }
                    //
                    FileBuilder.HandleFile();
                    //
                    foreach (var tuple in tableNames.Zip(tblschema, (x, y) => (x, y)))
                    {
                        Console.WriteLine($"{tuple.y}.{tuple.x}");
                        //Find same table name in various schemas
                        //TODO: Remove duplicate table names
                        tbls.Add(FileBuilder.ReadSqlServerSchema(tuple.x, tuple.y, con));
                    }
                    //

                    ds.Tables = tbls.ToList();
                    ds.Views = FileBuilder.ReadSqlServerViews(con);
                    Logger.Debug($"Finished Reading SQLServer Info");
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"{ex.Message}");
                Logger.Error(ex.StackTrace);
            }
            return ds;
        }

        #region GerateSQLiteDB
        private void GenerateSQLite(DatabaseSchema ds)
        {
            SQLiteFilePath = string.Format(@"{0}\{1}", Configuration["SQLiteFilePath"], Configuration["SQLiteFiles"]);
            Logger.Information($"Initiated SQLLite Generation {SQLiteFilePath}");
            //SQLiteConnection.CreateFile(SQLiteFilePath);
            string sqliteConnStr = CreateSQLiteConnectionString();
            using (SQLiteConnection con = new SQLiteConnection(sqliteConnStr))
            {
                con.Open();
                foreach (TableSchema t1 in ds.Tables)
                {
                    BuildSQLiteTable(con, t1);
                }
            }
        }

        private int BuildSQLiteTable(SQLiteConnection connectionStr, TableSchema ts)
        {
            int result = 0;
            string stm = BuildSQLiteCreateTabel(ts);
            Logger.Information($"Create Query \n {stm} \n");
            try
            {
                SQLiteCommand cmd = new SQLiteCommand(stm, connectionStr);
                result = cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                /*
                 * To continue code execution
                 * */
                Logger.Error($"Excpetion Occured at BuildSQLiteTable {ex.StackTrace} \n {ex.Message}");
                //throw;
            }
            finally
            {

            }

            return result;
        }

        private string BuildSQLiteCreateTabel(TableSchema ts)
        {
            StringBuilder sb = new StringBuilder();
            Logger.Debug($"Creating Table :: {ts.Table}");
            sb.Append($"CREATE TABLE [{ts.TableName}] (");
            bool pk = false;
            for (int i = 0; i < ts.Columns.Count; i++)
            {
                ColumnSchema col = ts.Columns[i];
                string colQ = BuildSQLiteCol(col, ts, ref pk);
                sb.Append(colQ);
                if (i < ts.Columns.Count - 1)
                    sb.Append(",\n");
                //call build col statement
            }
            //if (ts.PrimaryKey != null && ts.PrimaryKey.Count > 0 & !pk)
            //{
            //    sb.AppendLine(",");
            //    sb.AppendLine("PRIMARY KEY (");
            //    for (int i = 0; i < ts.PrimaryKey.Count; i++)
            //    {
            //        sb.Append("[" + ts.PrimaryKey[i] + "]");
            //        if (i < ts.PrimaryKey.Count - 1)
            //            sb.AppendLine(",");
            //    } // for
            //    sb.Append(")\n");
            //}
            //else
            //    sb.AppendLine("");

            // add foreign keys...
            if (ts.ForeignKeys.Count > 0)
            {
                sb.Append(",\n");
                for (int i = 0; i < ts.ForeignKeys.Count; i++)
                {
                    ForeignKeySchema foreignKey = ts.ForeignKeys[i];
                    string stmt = string.Format("    FOREIGN KEY ([{0}])\n        REFERENCES [{1}]([{2}])",
                                foreignKey.ColumnName, foreignKey.ForeignTableName, foreignKey.ForeignColumnName);

                    sb.Append(stmt);
                    if (i < ts.ForeignKeys.Count - 1)
                        sb.AppendLine(",");
                } // for
            }


            sb.Append(");\n");

            // Create any relevant indexes
            if (ts.Indexes != null)
            {
                for (int i = 0; i < ts.Indexes.Count; i++)
                {
                    string stmt = BuildCreateIndex(ts.TableName, ts.Indexes[i]);
                    sb.AppendLine(stmt + ";\n");
                } // for
            } // if

            string query = sb.ToString();
            return sb.ToString();
        }

        private string BuildSQLiteCol(ColumnSchema col, TableSchema ts, ref bool pkey)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append($" {col.ColumnName}");
            //if (col.IsIdentity)
            //{
            //    if (ts.PrimaryKey.Count == 1 &&
            //        (col.ColumnType == "tinyint" ||
            //        col.ColumnType == "int" ||
            //        col.ColumnType == "smallint" ||
            //        col.ColumnType == "bigint" ||
            //        col.ColumnType == "integer")
            //       )
            //    {
            //        sb.Append(" integer PRIMARY KEY");
            //        pkey = true;
            //    }
            //    else
            //        sb.Append(" integer");
            //}
            //else
            {
                if (col.ColumnType == "int")
                    sb.Append(" integer");
                else
                {
                    sb.Append($" " + col.ColumnType);
                }
                if (col.Length > 0)
                    sb.Append("(" + col.Length + ")");
            }
            if (!col.IsNullable)
                sb.Append(" NOT NULL");

            if (col.IsCaseSensitivite.HasValue && !col.IsCaseSensitivite.Value)
                sb.Append(" COLLATE NOCASE");

            string defval = FileBuilder.StripParens(col.DefaultValue);

            defval = FileBuilder.DiscardNational(defval);

            //Logger.Debug("DEFAULT VALUE BEFORE [" + col.DefaultValue + "] AFTER [" + defval + "]");

            if (defval != string.Empty && defval.ToUpper().Contains("GETDATE"))
            {
                //Logger.Debug("converted SQL Server GETDATE() to CURRENT_TIMESTAMP for column [" + col.ColumnName + "]");
                sb.Append(" DEFAULT (CURRENT_TIMESTAMP)");
            }
            else if (defval != string.Empty && FileBuilder.IsValidDefaultValue(defval))
                sb.Append(" DEFAULT " + defval);

            return sb.ToString();
        }

        private static string BuildCreateIndex(string tableName, IndexSchema indexSchema)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("CREATE ");
            if (indexSchema.IsUnique)
                sb.Append("UNIQUE ");
            sb.Append("INDEX [" + tableName + "_" + indexSchema.IndexName + "]\n");
            sb.Append("ON [" + tableName + "]\n");
            sb.Append("(");
            for (int i = 0; i < indexSchema.Columns.Count; i++)
            {
                sb.Append("[" + indexSchema.Columns[i].ColumnName + "]");
                if (!indexSchema.Columns[i].IsAscending)
                    sb.Append(" DESC");
                if (i < indexSchema.Columns.Count - 1)
                    sb.Append(", ");
            } // for
            sb.Append(")");

            return sb.ToString();
        }

        private string CreateSQLiteConnectionString()
        {
            Logger.Debug($"Creating SQLite Connection string file {SQLiteFilePath}");
            SQLiteConnectionStringBuilder builder = new SQLiteConnectionStringBuilder();
            builder.DataSource = SQLiteFilePath;
            builder.PageSize = 4096;
            builder.UseUTF16Encoding = true;
            return builder.ConnectionString;
        }
        #endregion


        #region CopySQLTuples2SQLite
        private void CopyTuples(string sqlConstr, string sqliteConnStr, ref DatabaseSchema ds)
        {
            Logger.Debug($"COPY SQL Tuples  {SQLiteFilePath}");
            SqlConnection sqlConn;
            SQLiteConnection sqliteConn;
            SqlCommand cmd;
            SqlDataReader dr = null;
            SQLiteCommand sqliteInsertCmd;


            using (sqlConn = new SqlConnection(sqlConstr))
            {
                sqlConn.Open();
                using (sqliteConn = new SQLiteConnection(sqliteConnStr))
                {
                    sqliteConn.Open();
                    foreach (TableSchema ts in ds.Tables)
                    {
                        if (sqliteConn.State == ConnectionState.Closed)
                            sqliteConn.Open();
                        Logger.Debug($"Preparing for copy data SQL > SQLite  {ts.Table}");
                        SQLiteTransaction sqliteTx = sqliteConn.BeginTransaction();
                        try
                        {
                            string selectQuery = SelectSQLTable(ts);
                            if (sqlConn.State == ConnectionState.Closed)
                                sqlConn.Open();
                            using (cmd = new SqlCommand(selectQuery, sqlConn))
                            {
                                using (dr = cmd.ExecuteReader())
                                {
                                    sqliteInsertCmd = BuildSQLiteInsert(ts);

                                    while (dr.Read())
                                    {
                                        sqliteInsertCmd.Connection = sqliteConn;
                                        sqliteInsertCmd.Transaction = sqliteTx;
                                        List<string> lstParams = new List<string>();
                                        ts.Columns.ForEach(c =>
                                        {
                                            string paramName = $"@{GetNormalizedName(c.ColumnName.Trim(), lstParams)}";
                                            lstParams.Add(paramName.Trim());
                                            Logger.Debug($"Paramter {paramName} value {dr[c.ColumnName].ToString().Trim()} {c.ColumnType.Trim()}");
                                            //sqliteInsertCmd.Parameters[paramName].Value = CastValForCol(dr[c.ColumnName], c);
                                            object _va = CastValForCol(dr[c.ColumnName], c);
                                            sqliteInsertCmd.Parameters.AddWithValue(paramName.Trim(), _va);

                                        });
                                        sqliteInsertCmd.ExecuteNonQuery();
                                    }
                                }
                                sqliteTx.Commit();
                            }
                        }
                        catch (SQLiteException sqliteex)
                        {
                            Logger.Error($"SQLite Excpetion {sqliteex.Message}");
                        }
                        catch (Exception ex)
                        {
                            Logger.Error($"Exception Occured at CopyTuples {ex.Message} \n {ex.StackTrace}");
                            sqliteTx.Rollback();
                            //throw;
                        }
                        finally
                        {
                            sqlConn.Close();
                            sqliteConn.Close();
                            if (dr != null)
                                dr.Close();
                        }

                    }
                }
            }
        }
        private SQLiteCommand BuildSQLiteInsert(TableSchema ts)
        {
            SQLiteCommand cmd = new SQLiteCommand();
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append($"INSERT INTO [{ts.TableName}] (");
                ts.Columns.ForEach(c =>
                {
                    //if (c.ColumnName.ToLower() != "id")
                    sb.Append($"[{c.ColumnName}],");
                });
                sb.Remove(sb.Length - 1, 1);
                sb.Append($") VALUES (");
                List<string> lstPname = new List<string>();
                ts.Columns.ForEach(c =>
                {
                    string pName = $"@{GetNormalizedName(c.ColumnName, lstPname)}";
                    sb.Append($"{pName},");
                    DbType dbType = GetDbTypeOfColumn(c);
                    SQLiteParameter param = new SQLiteParameter(pName, c.ColumnName);
                    cmd.Parameters.Add(param);

                    lstPname.Add(pName);
                });
                sb.Remove(sb.Length - 1, 1);
                sb.Append(")");
                cmd.CommandText = sb.ToString();
                cmd.CommandType = CommandType.Text;
            }
            catch (Exception ex)
            {
                Logger.Error($"Excpetion occured at BuildSQLiteInsert {ex.Message} / {ex.StackTrace}");

            }


            return cmd;
        }
        private object CastValForCol(object val, ColumnSchema column)
        {
            if (val is null)
                return null;
            DbType dt = GetDbTypeOfColumn(column);
            switch (dt)
            {
                case DbType.Int32:
                    if (val is short)
                        return (int)(short)val;
                    if (val is byte)
                        return (int)(byte)val;
                    if (val is long)
                        return (int)(long)val;
                    if (val is decimal)
                        return (int)(decimal)val;
                    break;

                case DbType.Int16:
                    if (val is int)
                        return (short)(int)val;
                    if (val is byte)
                        return (short)(byte)val;
                    if (val is long)
                        return (short)(long)val;
                    if (val is decimal)
                        return (short)(decimal)val;
                    break;

                case DbType.Int64:
                    if (val is int)
                        return (long)(int)val;
                    if (val is short)
                        return (long)(short)val;
                    if (val is byte)
                        return (long)(byte)val;
                    if (val is decimal)
                        return (long)(decimal)val;
                    break;

                case DbType.Single:
                    if (val is double)
                        return (float)(double)val;
                    if (val is decimal)
                        return (float)(decimal)val;
                    break;

                case DbType.Double:
                    if (val is float)
                        return (double)(float)val;
                    if (val is double)
                        return (double)val;
                    if (val is decimal)
                        return (double)(decimal)val;
                    break;

                case DbType.String:
                    if (val is Guid)
                        return ((Guid)val).ToString();
                    break;

                case DbType.Guid:
                    if (val is string)
                        return Guid.Parse((string)val);
                    if (val is byte[])
                        return new Guid((byte[])val);
                    break;

                case DbType.Binary:
                case DbType.Boolean:
                case DbType.DateTime:
                    break;

                default:
                    Logger.Error("argument exception - illegal database type");
                    throw new ArgumentException("Illegal database type [" + Enum.GetName(typeof(DbType), dt) + "]");
            }
            return val;
        }
        private DbType GetDbTypeOfColumn(ColumnSchema cs)
        {
            if (cs.ColumnType == "tinyint")
                return DbType.Byte;
            if (cs.ColumnType == "int")
                return DbType.Int32;
            if (cs.ColumnType == "smallint")
                return DbType.Int16;
            if (cs.ColumnType == "bigint")
                return DbType.Int64;
            if (cs.ColumnType == "bit")
                return DbType.Boolean;
            if (cs.ColumnType == "nvarchar" || cs.ColumnType == "varchar" ||
                cs.ColumnType == "text" || cs.ColumnType == "ntext")
                return DbType.String;
            if (cs.ColumnType == "float")
                return DbType.Double;
            if (cs.ColumnType == "real")
                return DbType.Single;
            if (cs.ColumnType == "blob")
                return DbType.Binary;
            if (cs.ColumnType == "numeric")
                return DbType.Double;
            if (cs.ColumnType == "timestamp" || cs.ColumnType == "datetime" || cs.ColumnType == "datetime2" || cs.ColumnType == "date" || cs.ColumnType == "time")
                return DbType.DateTime;
            if (cs.ColumnType == "nchar" || cs.ColumnType == "char")
                return DbType.String;
            if (cs.ColumnType == "uniqueidentifier" || cs.ColumnType == "guid")
                return DbType.Guid;
            if (cs.ColumnType == "xml")
                return DbType.String;
            if (cs.ColumnType == "sql_variant")
                return DbType.Object;
            if (cs.ColumnType == "integer")
                return DbType.Int64;

            Logger.Error("illegal db type found");
            throw new ApplicationException("Illegal DB type found (" + cs.ColumnType + ")");
        }
        private string SelectSQLTable(TableSchema table)
        {
            string selectQuery = string.Empty;
            try
            {


                StringBuilder sb = new StringBuilder();
                sb.Append(" SELECT ");
                foreach (ColumnSchema col in table.Columns)
                {
                    sb.Append($" [{col.ColumnName.Trim()}],");
                }

                sb.Remove(sb.Length - 1, 1);

                sb.Append($" FROM {table.Table}");

                selectQuery = sb.ToString().Trim();
                Logger.Information($"Select SQL Table {selectQuery}");
            }
            catch (Exception ex)
            {

                Logger.Error($"Exception occured at SelectSQLTable {ex.Message} / {ex.StackTrace}");
            }
            return selectQuery;
        }
        private static string GetNormalizedName(string str, List<string> names)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < str.Length; i++)
            {
                if (Char.IsLetterOrDigit(str[i]) || str[i] == '_')
                    sb.Append(str[i]);
                else
                    sb.Append("_");
            } // for

            // Avoid returning duplicate name

            if (names.Count > 0 && names.Contains(sb.ToString()))
                return GetNormalizedName(sb.ToString() + "_", names);
            else
                return sb.ToString();
        }
        #endregion
    }
}
