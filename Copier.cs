using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Text;

namespace CopyDb
{
	internal class Copier
	{
		private readonly DbInfo _source;
		private readonly DbInfo _dest;
		private readonly bool _overwriteExistingDatabase;
		private Queue<TableInfo> _queue;

		public Copier(DbInfo source, DbInfo dest, bool overwriteExistingDatabase)
		{
			_source = source;
			_dest = dest;
			_overwriteExistingDatabase = overwriteExistingDatabase;
		}

		public void Run()
		{
			using (SqlConnection sourcecn = _source.ConnectToExistingDatabase())
			{
				var tables = LoadTableSchema(sourcecn);
				_queue = new Queue<TableInfo>(tables.Values);
			}
			using (SqlConnection destcn = _dest.ConnectToNewDatabase(_overwriteExistingDatabase))
			{
			}

			var ex = TryCopyLoop();
			if (ex != null)
				throw ex;
		}

		private Exception TryCopyLoop()
		{
			try
			{
				CopyLoop();
				return null;
			}
			catch (Exception ex)
			{
				return ex;
			}
		}

		private void CopyLoop()
		{
			using (SqlConnection sourcecn = _source.ConnectToExistingDatabase())
			using (SqlConnection destcn = _dest.ConnectToExistingDatabase())
			{
				while (true)
				{
					TableInfo table;
					try
					{
						lock (_queue)
							table = _queue.Dequeue();
					}
					catch (InvalidOperationException)
					{
						return;
					}
					CopyTable(table, sourcecn, destcn);
				}
			}
		}

		private static void ExecuteSql(SqlConnection destcn, string sql)
		{
			using (SqlCommand cmd = Command.Sql.Create(destcn, sql))
			{
				try
				{
					cmd.ExecuteNonQuery();
				}
				catch
				{
					Console.WriteLine(sql);
					throw;
				}
			}
		}

		private static void CreateSchemaIfNecessary(string schema, SqlConnection destcn)
		{
			StringBuilder s = new StringBuilder(1024);
			using (TextWriter writer = new StringWriter(s))
				WriteSchemaDDL(schema, writer);
			ExecuteSql(destcn, s.ToString());
		}

		private static void CopyTable(TableInfo table, SqlConnection sourcecn, SqlConnection destcn)
		{
			Console.Write(table.Name);
			CreateTable(table, destcn);
			PreCopyTable(table, destcn);
			CopyTableData(table, sourcecn, destcn);
			PostCopyTable(table, destcn);
		}

		private static void CreateTable(TableInfo table, SqlConnection destcn)
		{
			CreateSchemaIfNecessary(table.Name.Schema, destcn);
			StringBuilder s = new StringBuilder(1024);
			using (TextWriter writer = new StringWriter(s))
				WriteTableDDL(table, writer);
			ExecuteSql(destcn, s.ToString());
		}

		private static void PreCopyTable(TableInfo table, SqlConnection destcn)
		{
			if (table.HasIdentity)
				ExecuteSql(destcn, String.Format("set IDENTITY_INSERT {0} on", table.Name));
		}

		private static void PostCopyTable(TableInfo table, SqlConnection destcn)
		{
			if (table.HasIdentity)
				ExecuteSql(destcn, String.Format("set IDENTITY_INSERT {0} off", table.Name));
		}

		private static void CopyTableData(TableInfo table, SqlConnection sourcecn, SqlConnection destcn)
		{
			string columnlist = GenerateColumns(table);

			string selectsql = String.Format("select {0} from {1}", columnlist, table.Name);
			using (SqlCommand selectcmd = Command.Sql.Create(sourcecn, selectsql))
			using (SqlDataReader dr = selectcmd.ExecuteReader(CommandBehavior.SingleResult | CommandBehavior.SequentialAccess))
			using (var copier = GetCopier(destcn, table))
			{
				copier.WriteToServer(dr);
			}
		}

		private static SqlBulkCopy GetCopier(SqlConnection destcn, TableInfo table)
		{
			SqlBulkCopy copier = new SqlBulkCopy(destcn, SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.KeepIdentity, null)
			{
				BulkCopyTimeout = 0,
				BatchSize = 10000,
				DestinationTableName = table.Name.ToString(),
				NotifyAfter = 1000,
			};
			copier.SqlRowsCopied += (o, args) => Console.Write(".");
			return copier;
		}

		#region Load

		private static IDictionary<TableName, TableInfo> LoadTableSchema(SqlConnection sourcecn)
		{
			var tables = new Dictionary<TableName, TableInfo>();
			string sql = Resource.LoadString("TableSchemaInfo.sql");
			using (SqlCommand cmd = Command.Sql.Create(sourcecn, sql))
			{
				using (SqlDataReader dr = cmd.ExecuteReader())
				{
					LoadColumnInfo(dr, tables);
					if (dr.NextResult())
						LoadPrimaryKeyInfo(dr, tables);
				}
			}
			return tables;
		}

		private static TableInfo LoadTableInfo(IDictionary<TableName, TableInfo> tables, TableName tablename)
		{
			TableInfo table;
			if (!tables.TryGetValue(tablename, out table))
			{
				table = new TableInfo(tablename);
				tables.Add(table.Name, table);
			}
			return table;
		}

		private static void LoadColumnInfo(SqlDataReader dr, IDictionary<TableName, TableInfo> tables)
		{
			while (dr.Read())
			{
				TableName tablename = new TableName((string)dr["SchemaName"], (string)dr["TableName"]);
				TableInfo table = LoadTableInfo(tables, tablename);
				ColumnInfo column = new ColumnInfo(dr["ColumnName"], dr["Type"], dr["Size"], dr["Precision"], dr["Scale"], dr["IsNullable"], dr["IsIdentity"], dr["IdentitySeed"], dr["IdentityIncrement"], dr["Calculation"], dr["Position"], dr["Collation"]);
				table.Columns.Add(column);
				table.HasIdentity |= column.IsIdentity;
			}
		}

		private static void LoadPrimaryKeyInfo(SqlDataReader dr, IDictionary<TableName, TableInfo> tables)
		{
			while (dr.Read())
			{
				TableName tablename = new TableName((string)dr["SchemaName"], (string)dr["TableName"]);
				TableInfo table = LoadTableInfo(tables, tablename);
				KeyInfo primarykey = table.PrimaryKey;
				if (primarykey == null)
				{
					primarykey = new KeyInfo(dr["ConstraintName"], dr["IsClustered"]);
					table.PrimaryKey = primarykey;
				}
				ColumnInfo column = table.Columns[table.ColumnIndex((string)dr["ColumnName"])];
				column.IsDescending = dr["IsDescending"].Equals(1);
				primarykey.Columns.Add(column);
			}
		}

		#endregion

		#region WriteDDL

		private static void WriteSchemaDDL(string schema, TextWriter writer)
		{
			writer.Write("if SCHEMA_ID('{0}') is null exec('create schema [{1}]')", schema, schema.Replace("]", "]]"));
		}

		private static void WriteTableDDL(TableInfo table, TextWriter writer)
		{
			writer.WriteLine("CREATE TABLE {0}", table.Name);
			writer.WriteLine("(");
			WriteColumnsDDL(table.Columns, writer);
			WritePrimaryKeyDDL(table.PrimaryKey, writer);
			writer.WriteLine();
			writer.WriteLine(")");
		}

		private static void WriteColumnsDDL(IEnumerable<ColumnInfo> columns, TextWriter writer)
		{
			bool needcomma = false;
			foreach (ColumnInfo column in columns)
			{
				if (needcomma)
					writer.WriteLine(",");
				else
					needcomma = true;
				WriteColumnDDL(column, writer);
			}
		}

		private static void WriteColumnDDL(ColumnInfo column, TextWriter writer)
		{
			writer.Write("\t[{0}] ", column.Name);

			if (column.Calculation == null)
			{
				writer.Write(column.Type.ToString().ToLower());
				if (column.Size > -1)
					writer.Write("({0})", column.Size);
				if (column.IsMaxSize)
					writer.Write("(MAX)");
				else if (column.Precision > -1 && column.Scale > -1)
					writer.Write("({0},{1})", column.Precision, column.Scale);
				if (column.Collation != null)
					writer.Write(" COLLATE {0}", column.Collation);
				writer.Write(column.IsNullable ? " NULL" : " NOT NULL");
				if (column.IsIdentity)
					writer.Write(" IDENTITY({0},{1})", column.IdentitySeed, column.IdentityIncrement);
			}
			else
			{
				writer.Write(" AS {0}", column.Calculation);
			}
		}

		private static void WritePrimaryKeyDDL(KeyInfo primarykey, TextWriter writer)
		{
			if (primarykey == null) return;
			writer.WriteLine(",");
			writer.Write("\tCONSTRAINT [{0}] PRIMARY KEY", primarykey.Name);
			writer.Write(primarykey.IsClustered ? " CLUSTERED" : " NONCLUSTERED");
			writer.Write(" ( ");
			WritePrimaryKeyColumnsDDL(primarykey.Columns, writer);
			writer.Write(" )");
		}

		private static void WritePrimaryKeyColumnsDDL(IEnumerable<ColumnInfo> columns, TextWriter writer)
		{
			bool needcomma = false;
			foreach (ColumnInfo column in columns)
			{
				if (needcomma)
					writer.Write(", ");
				else
					needcomma = true;
				writer.Write("[{0}]", column.Name);
				if (column.IsDescending)
					writer.Write(" DESC");
			}
		}
		#endregion

		#region Write DML
		private static string GenerateColumns(TableInfo table)
		{
			return GenerateColumns(table, "[{0}]");
		}

		private static string GenerateColumns(TableInfo table, string format)
		{
			StringBuilder s = new StringBuilder(1024);
			bool needcomma = false;
			foreach (ColumnInfo column in table.Columns)
			{
				if (column.Calculation != null) continue;
				if (needcomma)
					s.Append(",");
				else
					needcomma = true;
				if (format == null)
					s.Append(column.Name);
				else
					s.AppendFormat(format, column.Name);
			}
			return s.ToString();
		}
		#endregion
	}
}