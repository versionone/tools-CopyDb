using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Text;
using System.Threading;

namespace CopyDb
{
	internal class Copier
	{
		private const int ThreadCount = 4;
		private readonly DbInfo _source;
		private readonly DbInfo _dest;
		private readonly bool _overwriteExistingDatabase;
		private Queue<TableInfo> _queue;
		private volatile bool _abort;
		private Exception _exception;

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
			_dest.CreateDatabase(_overwriteExistingDatabase);

			var threads = new Thread[ThreadCount];
			for (var i = 0; i < threads.Length; ++i)
				threads[i] = new Thread(TryCopyLoop);
			foreach (var thread in threads)
				thread.Start();
			foreach (var thread in threads)
				thread.Join();

			if (_exception != null)
				throw _exception;
		}

		private void TryCopyLoop()
		{
			try
			{
				CopyLoop();
			}
			catch (Exception ex)
			{
				_abort = true;
				_exception = ex;
			}
		}

		private void CopyLoop()
		{
			using (SqlConnection sourcecn = _source.ConnectToExistingDatabase())
			using (SqlConnection destcn = _dest.ConnectToExistingDatabase())
			{
				while (!_abort)
				{
					TableInfo table;
					try
					{
						lock (_queue)
							table = _queue.Dequeue();
					}
					catch (InvalidOperationException)
					{
						return;	// queue is empty
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

		private void CopyTable(TableInfo table, SqlConnection sourcecn, SqlConnection destcn)
		{
			Console.Write(" " + table.Name.Name + " ");
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
			ClusterTable(table, destcn);
		}

		private static void ClusterTable(TableInfo table, SqlConnection destcn)
		{
			StringBuilder s = new StringBuilder(1024);
			using (TextWriter writer = new StringWriter(s))
				WriteClusterDDL(table, writer);
			if (s.Length > 0)
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

		private void CopyTableData(TableInfo table, SqlConnection sourcecn, SqlConnection destcn)
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

		private SqlBulkCopy GetCopier(SqlConnection destcn, TableInfo table)
		{
			SqlBulkCopy copier = new SqlBulkCopy(destcn, SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.KeepIdentity, null)
			{
				BulkCopyTimeout = 0,
				BatchSize = 10000,
				DestinationTableName = table.Name.ToString(),
				NotifyAfter = 1000,
			};
			copier.SqlRowsCopied += this.OnSqlRowsCopied;
			return copier;
		}

		private void OnSqlRowsCopied(object o, SqlRowsCopiedEventArgs args)
		{
			if (_abort)
				args.Abort = true;
			else
				Console.Write(".");
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
						LoadClusterInfo(dr, tables);
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

		private static void LoadClusterInfo(SqlDataReader dr, IDictionary<TableName, TableInfo> tables)
		{
			while (dr.Read())
			{
				TableName tablename = new TableName((string)dr["SchemaName"], (string)dr["TableName"]);
				TableInfo table = LoadTableInfo(tables, tablename);
				KeyInfo cluster = table.Cluster;
				if (cluster == null)
				{
					cluster = new KeyInfo(
						dr["ConstraintName"],
						dr["IsClustered"],
						dr["IsPrimaryKey"],
						dr["IsUnique"],
						dr["IsConstraint"],
						dr["IgnoreDupKey"],
						dr["FillFactor"],
						dr["PadIndex"],
						dr["AllowRowLocks"],
						dr["AllowPageLocks"]);
					table.Cluster = cluster;
				}
				ColumnInfo column = table.Columns[table.ColumnIndex((string)dr["ColumnName"])];
				column.IsDescending = dr["IsDescending"].Equals(1);
				cluster.Columns.Add(column);
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

		private static void WriteClusterDDL(TableInfo table, TextWriter writer)
		{
			var cluster = table.Cluster;
			if (cluster == null) return;
			if (cluster.IsConstraint)
			{
				// alter table <table> add constraint <name> {primary key | unique} {clustered | nonclustered} (<columns>)
				writer.Write("alter table {0} add constraint [{1}]", table.Name, cluster.Name);
				if (cluster.IsPrimaryKey)
					writer.Write(" primary key");
				else if (cluster.IsUnique)
					writer.Write(" unique");
				writer.Write(cluster.IsClustered ? " clustered" : " nonclustered");
				writer.Write(" (");
				WriteClusterColumnsDDL(cluster.Columns, writer);
				writer.Write(")");
			}
			else
			{
				// create [unique] {clustered | nonclustered} index <name> on <table> (<columns>)
				writer.Write("create");
				if (cluster.IsUnique)
					writer.Write(" unique");
				writer.Write(cluster.IsClustered ? " clustered" : " nonclustered");
				writer.Write(" index [{1}] on {0}", table.Name, cluster.Name);
				writer.Write(" (");
				WriteClusterColumnsDDL(cluster.Columns, writer);
				writer.Write(")");
			}

			// with (IGNORE_DUP_KEY={ON|OFF})
			writer.Write(" with (");
			writer.Write("IGNORE_DUP_KEY=");
			writer.Write(cluster.IgnoreDupKey ? "ON" : "OFF");
			if (cluster.FillFactor > 0)
			{
				writer.Write(",FILLFACTOR=");
				writer.Write(cluster.FillFactor);
			}
			writer.Write(",PAD_INDEX=");
			writer.Write(cluster.PadIndex ? "ON" : "OFF");
			writer.Write(",ALLOW_ROW_LOCKS=");
			writer.Write(cluster.AllowRowLocks ? "ON" : "OFF");
			writer.Write(",ALLOW_PAGE_LOCKS=");
			writer.Write(cluster.AllowPageLocks ? "ON" : "OFF");
			writer.Write(")");
		}

		private static void WriteClusterColumnsDDL(IEnumerable<ColumnInfo> columns, TextWriter writer)
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
