using System;
using System.Collections;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Text;

namespace CopyDb
{
	class DbInfo
	{
		public string Server;
		public string Database;
		public string Username;
		public string Password;

		public DbInfo (string arg)
		{
			string[] parts = arg.Split(';');
			if (parts.Length < 2)
				throw new ArgumentException("Argument is not in the format Server;Database[;Username;Password]");

			Server = parts[0];
			Database = parts[1];

			if (parts.Length >= 4)
			{
				Username = parts[2];
				Password = parts[3];
			}
		}

		public string ConnectionString
		{
			get
			{
				string cnstr = "Server=" + Server;
				if (Username == null)
					cnstr += ";Integrated Security=true";
				else
					cnstr += ";User ID=" + Username + ";Password=" + Password;
				return cnstr;
			}
		}

		public SqlConnection ConnectToExistingDatabase ()
		{
			SqlConnection cn = new SqlConnection(ConnectionString);
			cn.Open();
			cn.ChangeDatabase(Database);
			return cn;
		}

		public SqlConnection ConnectToNewDatabase(bool deleteExisting)
		{
			SqlConnection cn = new SqlConnection(ConnectionString);
			cn.Open();
			if (deleteExisting)
				using (SqlCommand cmd = new SqlCommand("if exists (select * from sys.databases where name='" + Database + "') drop database [" + Database + "]", cn))
					cmd.ExecuteNonQuery();
			using (SqlCommand cmd = new SqlCommand("create database [" + Database + "]", cn))
				cmd.ExecuteNonQuery();
			cn.ChangeDatabase(Database);
			return cn;
		}

	}

	class TableInfo
	{
		public readonly string Name;
		public readonly IList Columns = new ArrayList();
		public KeyInfo PrimaryKey;
		public bool HasIdentity;

		public TableInfo (string name)
		{
			Name = name;
		}

		public int ColumnIndex (string name)
		{
			for (int i = 0; i < Columns.Count; ++i)
				if ( ((ColumnInfo)Columns[i]).Name == name)
					return i;
			return -1;
		}

		public ColumnInfo Column (int index)
		{
			return (ColumnInfo) Columns[index];
		}

		private class _Comparer : IComparer
		{
			public int Compare (object x, object y)
			{
				return Compare((TableInfo) x, (TableInfo) y);
			}

			private static int Compare (TableInfo x, TableInfo y)
			{
				return string.Compare(x.Name, y.Name);
			}
		}
		public static readonly IComparer Comparer = new _Comparer();
	}

	class ColumnInfo
	{
		public readonly string Name;
		public readonly SqlDbType Type;
		public readonly int Size;
		public readonly int Precision;
		public readonly int Scale;
		public readonly bool IsNullable;
		public readonly bool IsIdentity;
		public readonly int IdentitySeed;
		public readonly int IdentityIncrement;
		public readonly string Calculation;
		public readonly int Position;
		public readonly string Collation;
		public bool IsDescending = false;

		public ColumnInfo (string name, SqlDbType type, int size, int precision, int scale, bool isnullable, bool isidentity, int identityseed, int identityincr, string calculation, int position, string collation)
		{
			Name = name;
			Type = type;
			Size = size;
			Precision = precision;
			Scale = scale;
			IsNullable = isnullable;
			IsIdentity = isidentity;
			IdentitySeed = identityseed;
			IdentityIncrement = identityincr;
			Calculation = calculation;
			Position = position;
			Collation = collation;
		}

		public ColumnInfo (object name, object type, object size, object precision, object scale, object isnullable, object isidentity, object identityseed, object identityincr, object calculation, object position, object collation)
			: this(
				(string) name, 
				(SqlDbType) Enum.Parse(typeof(SqlDbType), (string) type, true),
				size.Equals(DBNull.Value)? -1: (int) size,
				precision.Equals(DBNull.Value)? -1: (int) precision,
				scale.Equals(DBNull.Value)? -1: (int) scale,
				(bool) isnullable,
				(bool) isidentity,
				identityseed.Equals(DBNull.Value)? 0: (int) identityseed,
				identityincr.Equals(DBNull.Value)? 0: (int) identityincr,
				(calculation.Equals(DBNull.Value))? null: (string) calculation,
				(int) position,
				collation.Equals(DBNull.Value)? null: (string) collation
			)
		{}

		public bool IsMaxSize
		{
			get { return Size == -1 && (Type == SqlDbType.NVarChar || Type == SqlDbType.VarBinary || Type == SqlDbType.VarChar); }
		}
	}

	class KeyInfo
	{
		public readonly string Name;
		public readonly bool IsClustered;
		public readonly IList Columns = new ArrayList();

		public KeyInfo (string name, bool isclustered)
		{
			Name = name;
			IsClustered = isclustered;
		}

		public KeyInfo (object name, object isclustered)
			: this(
				(string) name,
				(bool) isclustered
			)
		{}
	}


	class Global
	{
		[STAThread]
		static int Main (string[] args)
		{
			Stopwatch sw = new Stopwatch();
			sw.Start();

			bool overwriteExistingDatabase = false;
			DbInfo source = null;
			DbInfo dest = null;

			var positional = 0;
			foreach (var arg in args)
			{
				if (arg[0] == '-' || arg[0] == '/')
				{
					if (arg.Length < 2)
						return ShowUsage();
					switch (arg[1])
					{
						case '?':
						case 'h':
							return ShowUsage();
						case 'f':
							overwriteExistingDatabase = true;
							break;
						default:
							return ShowUsage();
					}
				}
				else
				{
					switch (++positional)
					{
						case 1:
							source = new DbInfo(arg);
							break;
						case 2:
							dest = new DbInfo(arg);
							break;
					}
				}
			}
			if (positional < 2)
				return ShowUsage();

			using (SqlConnection sourcecn = source.ConnectToExistingDatabase())
			{
				IDictionary tables = LoadTableSchema(sourcecn);

				using (SqlConnection destcn = dest.ConnectToNewDatabase(overwriteExistingDatabase))
				{
					foreach (TableInfo table in tables.Values)
					{
						Console.Write("Table " + table.Name);
						CreateTable(table, destcn);
						PreCopyTable(table, destcn);
						CopyTable(table, sourcecn, destcn);
						PostCopyTable(table, destcn);
						Console.WriteLine();
					}
/*
				TableInfo[] atables = new TableInfo[tables.Count];
				tables.Values.CopyTo(atables, 0);
				Array.Sort(atables, TableInfo.Comparer);
				WriteTablesDDL(atables, Console.Out);
*/
//					WriteTablesDDL(tables.Values, Console.Out);
//					WriteTablesDML(tables.Values,  sourcecn, Console.Out);

/*
				Schema.Descriptor[] tables = Schema.GetObjectNames(new ObjectType[] {ObjectType.Table}, sourcecn, null);
				Array.Sort(tables, Schema.Descriptor.Comparer);
				foreach (Schema.Descriptor table in tables)
				{
					Scripter.Script(table, Console.Out, sourcecn, null);
				}
*/
				}
			}

			sw.Stop();
			Console.WriteLine("Elapsed time = {0:0.000} seconds", sw.Elapsed.TotalSeconds);
			return 0;
		}

		private static void ExecuteSql (SqlConnection destcn, string sql)
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

		private static void CreateTable (TableInfo table, SqlConnection destcn)
		{
			StringBuilder s = new StringBuilder(1024);
			using (TextWriter writer = new StringWriter(s))
				WriteTableDDL(table, writer);
			ExecuteSql(destcn, s.ToString());
		}

		private static void PreCopyTable (TableInfo table, SqlConnection destcn)
		{
			if (table.HasIdentity)
				ExecuteSql(destcn, string.Format("set IDENTITY_INSERT [{0}] on", table.Name));
		}

		private static void PostCopyTable (TableInfo table, SqlConnection destcn)
		{
			if (table.HasIdentity)
				ExecuteSql(destcn, string.Format("set IDENTITY_INSERT [{0}] off", table.Name));
		}

		private static void CopyTable (TableInfo table, SqlConnection sourcecn, SqlConnection destcn)
		{
			string columnlist = GenerateColumns(table);

			string selectsql = string.Format("select {0} from [{1}]", columnlist, table.Name);
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
				DestinationTableName = "[" + table.Name + "]",
				NotifyAfter = 1000,
			};
			copier.SqlRowsCopied += (o, args) => Console.Write(".");
			return copier;
		}

		#region Load
		private static IDictionary LoadTableSchema (SqlConnection sourcecn)
		{
			Hashtable tables = new Hashtable();
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

		private static TableInfo LoadTableInfo (IDictionary tables, string tablename)
		{
			TableInfo table = (TableInfo) tables[ tablename ];
			if (table == null)
			{
				table = new TableInfo(tablename);
				tables.Add(table.Name,  table);
			}
			return table;
		}

		private static void LoadColumnInfo (SqlDataReader dr, IDictionary tables)
		{
			while (dr.Read())
			{
				TableInfo table = LoadTableInfo(tables, (string) dr["TableName"]);
				ColumnInfo column = new ColumnInfo(dr["ColumnName"], dr["Type"], dr["Size"], dr["Precision"], dr["Scale"], dr["IsNullable"], dr["IsIdentity"], dr["IdentitySeed"], dr["IdentityIncrement"], dr["Calculation"], dr["Position"], dr["Collation"]);
				table.Columns.Add(column);
				table.HasIdentity |= column.IsIdentity;
			}
		}

		private static void LoadPrimaryKeyInfo (SqlDataReader dr, IDictionary tables)
		{
			while (dr.Read())
			{
				TableInfo table = LoadTableInfo(tables, (string) dr["TableName"]);
				KeyInfo primarykey = table.PrimaryKey;
				if (primarykey == null)
				{
					primarykey = new KeyInfo(dr["ConstraintName"], dr["IsClustered"]);
					table.PrimaryKey = primarykey;
				}
				ColumnInfo column = (ColumnInfo) table.Columns[ table.ColumnIndex( (string) dr["ColumnName"]) ];
				column.IsDescending = dr["IsDescending"].Equals(1);
				primarykey.Columns.Add(column);
			}
		}
		#endregion

		#region WriteDDL
		private static void WriteTablesDDL (ICollection tables, TextWriter writer)
		{
			foreach (TableInfo table in tables)
				WriteTableDDL(table, writer);
		}

		private static void WriteTableDDL (TableInfo table, TextWriter writer)
		{
			writer.WriteLine("CREATE TABLE [{0}]", table.Name);
			writer.WriteLine("(");
			WriteColumnsDDL(table.Columns, writer);
			WritePrimaryKeyDDL(table.PrimaryKey, writer);
			writer.WriteLine();
			writer.WriteLine(")");
		}

		private static void WriteColumnsDDL (ICollection columns, TextWriter writer)
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

		private static void WriteColumnDDL (ColumnInfo column, TextWriter writer)
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
				writer.Write(column.IsNullable? " NULL": " NOT NULL");
				if (column.IsIdentity)
					writer.Write(" IDENTITY({0},{1})", column.IdentitySeed,  column.IdentityIncrement);
			}
			else
			{
				writer.Write(" AS {0}", column.Calculation);
			}
		}

		private static void WritePrimaryKeyDDL (KeyInfo primarykey, TextWriter writer)
		{
			if (primarykey == null) return;
			writer.WriteLine(",");
			writer.Write("\tCONSTRAINT [{0}] PRIMARY KEY", primarykey.Name);
			writer.Write(primarykey.IsClustered? " CLUSTERED": " NONCLUSTERED");
			writer.Write(" ( ");
			WritePrimaryKeyColumnsDDL(primarykey.Columns,  writer);
			writer.Write(" )");
		}

		private static void WritePrimaryKeyColumnsDDL (ICollection columns, TextWriter writer)
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
		private static string GenerateColumns (TableInfo table)
		{
			return GenerateColumns(table, "[{0}]");
		}

		private static string GenerateColumns (TableInfo table, string format)
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

		private static int ShowUsage ()
		{
			Console.WriteLine("Usage: CopyDb fromServer;fromDb[;usr;pwd] toServer;toDb[;usr;pwd]");
			Console.WriteLine("\t-f\tForce overwriting existing destination database");
			return 1;
		}
	}
}
