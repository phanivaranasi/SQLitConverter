using Microsoft.Extensions.Configuration;
using Serilog;
using SQLiteDump.Job.SQLiteAccess;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace SQLiteDump.Job.SQLiteBuilder
{
    public class FileBuilder : IFileBuilder
    {
        IConfiguration Configuration;
        ILogger Logger;
        SqlCommand cmd;
        SqlDataReader dr;
        List<TableSchema> tables = new List<TableSchema>();
        Regex _defaultValueRx = new Regex(@"\(N(\'.*\')\)");
        Regex _keyRx = new Regex(@"(([a-zA-Z_äöüÄÖÜß0-9\.]|(\s+))+)(\(\-\))?");
        Regex removedbo = new Regex(@"dbo\.", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        //public Dictionary<string, string> Files = new Dictionary<string, string>();
        string FilePath;
        public FileBuilder(IConfiguration configuration, ILogger logger)
        {
            Configuration = configuration;
            Logger = logger;
            Logger.Information("SQLite Conversion Initated");
        }

        public void HandleFile()
        {
            //Files = Configuration.GetSection("SQLiteFiles").GetChildren()
            //        .ToDictionary(n => n.Key, n => n.Value);
            FilePath = string.Format(@"{0}\{1}", Configuration["SQLiteFilePath"], Configuration["SQLiteFiles"]);
            if (File.Exists(FilePath))
                File.Delete(FilePath);
        }

        public TableSchema ReadSqlServerSchema(string tableName, string schema, SqlConnection con)
        {
            TableSchema ts = GetTableSchema(tableName, schema, ref con);
            Logger.Information($"Reading schema of table {ts.Table}");


            return ts;
        }

        public List<ViewSchema> ReadSqlServerViews(SqlConnection con)
        {
            List<ViewSchema> views = new List<ViewSchema>();
            Logger.Debug($"Reading SQL Server Views");
            try
            {
                using (cmd = new SqlCommand(SQLQuery.GetSQLViews, con))
                {
                    using (dr = cmd.ExecuteReader())
                    {
                        while (dr.Read())
                        {
                            if (dr["TABLE_NAME"] != DBNull.Value && dr["TABLE_NAME"].ToString() != null)
                            {

                                ViewSchema vs = new ViewSchema();
                                vs.ViewName = dr["TABLE_NAME"].ToString();
                                vs.ViewSQL = removedbo.Replace(dr["VIEW_DEFINITION"].ToString(), string.Empty);
                                views.Add(vs);
                            }

                        }

                    }
                }
            }
            catch (Exception ex)
            {
                throw;
            }
            Logger.Debug($"Found {views.Count} Views ");
            return views;
        }
        private static bool IsSingleQuoted(string value)
        {
            value = value.Trim();
            if (value.StartsWith("'") && value.EndsWith("'"))
                return true;
            return false;
        }
        public bool IsValidDefaultValue(string value)
        {
            if (IsSingleQuoted(value))
                return true;

            double testnum;
            if (!double.TryParse(value, out testnum))
                return false;
            return true;
        }
        public string StripParens(string value)
        {
            Regex rx = new Regex(@"\(([^\)]*)\)");
            Match m = rx.Match(value);
            if (!m.Success)
                return value;
            else
                return StripParens(m.Groups[1].Value);
        }
        public string DiscardNational(string value)
        {
            Regex rx = new Regex(@"N\'([^\']*)\'");
            Match m = rx.Match(value);
            if (m.Success)
                return m.Groups[1].Value;
            else
                return value;
        }

        #region PrivateMethods

        private TableSchema GetTableSchema(string tableName, string schema, ref SqlConnection con)
        {


            TableSchema tblSch = new TableSchema();
            tblSch.TableName = tableName;
            tblSch.TableSchemaName = schema;
            Logger.Information($"\nGet table schema {tblSch.Table} DB State {con.State}");
            tblSch.Columns = new List<ColumnSchema>();
            cmd = new SqlCommand(string.Format(SQLQuery.GetTableCols, tableName,schema), con);
            using (dr = cmd.ExecuteReader())
            {
                while (dr.Read())
                {
                    object tmp = dr["COLUMN_NAME"];
                    if (tmp is DBNull)
                        continue;
                    string colName = (string)dr["COLUMN_NAME"];
                    string colDefault;
                    if (tmp is DBNull)
                        colDefault = string.Empty;
                    else
                        colDefault = (string)tmp;
                    tmp = dr["IS_NULLABLE"];
                    bool isNullable = ((string)tmp == "YES");
                    string dataType = (string)dr["DATA_TYPE"];
                    bool isIdentity = false;
                    if (dr["IDENT"] != DBNull.Value)
                        isIdentity = ((int)dr["IDENT"]) == 1 ? true : false;
                    int length = dr["CSIZE"] != DBNull.Value ? Convert.ToInt32(dr["CSIZE"]) : 0;
                    ValidateDataType(dataType);
                    if (dataType == "timestamp")
                        dataType = "blob";
                    else if (dataType == "datetime" || dataType == "smalldatetime" || dataType == "date" || dataType == "datetime2" || dataType == "time")
                        dataType = "datetime";
                    else if (dataType == "decimal")
                        dataType = "numeric";
                    else if (dataType == "money" || dataType == "smallmoney")
                        dataType = "numeric";
                    else if (dataType == "binary" || dataType == "varbinary" ||
                        dataType == "image")
                        dataType = "blob";
                    else if (dataType == "tinyint")
                        dataType = "smallint";
                    else if (dataType == "bigint")
                        dataType = "integer";
                    else if (dataType == "sql_variant")
                        dataType = "blob";
                    else if (dataType == "xml")
                        dataType = "varchar";
                    else if (dataType == "uniqueidentifier")
                        dataType = "guid";
                    else if (dataType == "ntext")
                        dataType = "text";
                    else if (dataType == "nchar")
                        dataType = "char";

                    if (dataType == "bit" || dataType == "int")
                    {
                        if (colDefault == "('False')")
                            colDefault = "(0)";
                        else if (colDefault == "('True')")
                            colDefault = "(1)";
                    }

                    colDefault = FixDefaultValueString(colDefault);

                    ColumnSchema col = new ColumnSchema();
                    col.ColumnName = colName;
                    col.ColumnType = dataType;
                    col.Length = length;
                    col.IsNullable = isNullable;
                    col.IsIdentity = isIdentity;
                    col.DefaultValue = AdjustDefaultValue(colDefault);

                    tblSch.Columns.Add(col);

                }
            }//end data reader
            ReadPrimaryKeys(ref tblSch, ref con);
            ReadTableColumnCollate(ref tblSch, ref con);
            ReadTableIndex(ref tblSch, ref con);
            ReadForeignKeySchema(ref tblSch, ref con);

            Log.Debug($"Parsed informatino for {tblSch.Table}");

            return tblSch;
        }

        private void ReadForeignKeySchema(ref TableSchema tblSch, ref SqlConnection con)
        {
            tblSch.ForeignKeys = new List<ForeignKeySchema>();
            Logger.Debug($"Reading foreign key information {tblSch.Table}");
            string query = string.Format(SQLQuery.GetTableForeignKeys, tblSch.Table);
            using (cmd = new SqlCommand(query, con))
            {
                using (dr = cmd.ExecuteReader())
                {
                    while (dr.Read())
                    {
                        ForeignKeySchema schema = new ForeignKeySchema();
                        schema.ColumnName = (string)dr["ColumnName"];
                        schema.ForeignTableName = (string)dr["ForeignTableName"];
                        schema.ForeignColumnName = (string)dr["ForeignColumnName"];
                        schema.CascadeOnDelete = (string)dr["DeleteRule"] == "CASCADE";
                        schema.IsNullable = (string)dr["IsNullable"] == "YES";
                        schema.TableName = tblSch.TableName;
                        tblSch.ForeignKeys.Add(schema);
                    }
                }
            }
        }

        private void ReadTableIndex(ref TableSchema tblSch, ref SqlConnection con)
        {
            Logger.Debug($"Read table index info {tblSch.TableName}");
            string query = string.Format(SQLQuery.GetTableIndexInfo, tblSch.Table);
            cmd = new SqlCommand(query, con);
            tblSch.Indexes = new List<IndexSchema>();
            using (dr = cmd.ExecuteReader())
            {
                while (dr.Read())
                {
                    Logger.Debug($"index of  {dr["index_name"].ToString()}{dr["index_description"].ToString()}{dr["index_keys"].ToString()}");

                    if (dr["index_description"].ToString().Contains("primary key"))
                        continue;

                    IndexSchema indexSch = BuildIndexSchema(dr["index_name"].ToString(), dr["index_description"].ToString(), dr["index_keys"].ToString());
                    tblSch.Indexes.Add(indexSch);
                }
            }
        }

        private IndexSchema BuildIndexSchema(string indexName, string desc, string key)
        {
            IndexSchema schema = new IndexSchema();
            Logger.Debug($"\nIndex schema {indexName}/{desc}/{key}");

            string[] descParts = desc.Split(',');

            foreach (string p in descParts)
            {
                if (p.Trim().Contains("unique"))
                {
                    schema.IsUnique = true;
                    break;
                }
            } // foreach


            schema.Columns = new List<IndexColumn>();
            string[] keysParts = key.Split(',');
            foreach (string p in keysParts)
            {
                Match m = _keyRx.Match(p.Trim());
                if (!m.Success)
                {
                    throw new ApplicationException("Illegal key name [" + p + "] in index [" +
                        indexName + "]");
                }

                string key1 = m.Groups[1].Value;
                IndexColumn ic = new IndexColumn();
                ic.ColumnName = key1;
                if (m.Groups[2].Success)
                    ic.IsAscending = false;
                else
                    ic.IsAscending = true;

                schema.Columns.Add(ic);
            } // foreach

            Logger.Debug(Newtonsoft.Json.JsonConvert.SerializeObject(schema));
            return schema;
        }

        private void ReadTableColumnCollate(ref TableSchema tblSch, ref SqlConnection con)
        {
            Logger.Debug($"Read table collage info {tblSch.TableName}");
            string query = string.Format(SQLQuery.GetTableCollateInfo, tblSch.Table);
            cmd = new SqlCommand(query, con);
            using (dr = cmd.ExecuteReader())
            {
                while (dr.Read())
                {
                    bool? isCaseSensitive = null;
                    string colName = dr["name"].ToString();
                    if (dr["tds_collation"] != DBNull.Value)
                    {
                        byte[] mask = (byte[])dr["tds_collation"];
                        if ((mask[2] & 0x10) != 0)
                            isCaseSensitive = false;
                        else
                            isCaseSensitive = true;
                    }
                    if (isCaseSensitive.HasValue)
                    {
                        List<ColumnSchema> dt = (from item in tblSch.Columns
                                                 let c1 = item.IsCaseSensitivite = item.ColumnName == colName ? isCaseSensitive : isCaseSensitive
                                                 select item).ToList();
                        tblSch.Columns = dt;
                    }
                }
            }
        }

        private void ReadPrimaryKeys(ref TableSchema tblSch, ref SqlConnection con)
        {
            Logger.Debug($"Read Primary Keys {tblSch.TableName}");
            cmd = new SqlCommand(string.Format(SQLQuery.GetTableKeys, tblSch.TableName), con);
            tblSch.PrimaryKey = new List<string>();
            using (dr = cmd.ExecuteReader())
            {
                while (dr.Read())
                {
                    tblSch.PrimaryKey.Add(dr["COLUMN_NAME"].ToString());
                }
            }
        }

        private string AdjustDefaultValue(string val)
        {
            if (val == null || val == string.Empty)
                return val;

            Match m = _defaultValueRx.Match(val);
            if (m.Success)
                return m.Groups[1].Value;
            return val;
        }

        private string FixDefaultValueString(string colDefault)
        {
            bool replaced = false;
            string res = colDefault.Trim();

            // Find first/last indexes in which to search
            int first = -1;
            int last = -1;
            for (int i = 0; i < res.Length; i++)
            {
                if (res[i] == '\'' && first == -1)
                    first = i;
                if (res[i] == '\'' && first != -1 && i > last)
                    last = i;
            } // for

            if (first != -1 && last > first)
                return res.Substring(first, last - first + 1);

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < res.Length; i++)
            {
                if (res[i] != '(' && res[i] != ')')
                {
                    sb.Append(res[i]);
                    replaced = true;
                }
            }
            if (replaced)
                return "(" + sb.ToString() + ")";
            else
                return sb.ToString();
        }

        private void ValidateDataType(string dataType)
        {
            if (dataType == "int" || dataType == "smallint" ||
                dataType == "bit" || dataType == "float" ||
                dataType == "real" || dataType == "nvarchar" ||
                dataType == "varchar" || dataType == "timestamp" ||
                dataType == "varbinary" || dataType == "image" ||
                dataType == "text" || dataType == "ntext" ||
                dataType == "bigint" ||
                dataType == "char" || dataType == "numeric" ||
                dataType == "binary" || dataType == "smalldatetime" ||
                dataType == "smallmoney" || dataType == "money" ||
                dataType == "tinyint" || dataType == "uniqueidentifier" ||
                dataType == "xml" || dataType == "sql_variant" || dataType == "datetime2" || dataType == "date" || dataType == "time" ||
                dataType == "decimal" || dataType == "nchar" || dataType == "datetime")
                return;
            throw new ApplicationException("Validation failed for data type [" + dataType + "]");
        }


        #endregion
    }
}
