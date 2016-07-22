using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

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

	class TableName
	{
		public readonly string Schema;
		public readonly string Name;

		public TableName(string schema, string name)
		{
			Schema = schema;
			Name = name;
		}

		public override string ToString()
		{
			return FullyQualifiedName;
		}

		public string FullyQualifiedName
		{
			get { return Quoted(Schema) + "." + Quoted(Name); }
		}

		private string Quoted(string name)
		{
			return "[" + name.Replace("]", "]]") + "]";
		}

		public bool Equals(TableName other)
		{
			if (ReferenceEquals(null, other)) return false;
			if (ReferenceEquals(this, other)) return true;
			return Equals(other.Schema, Schema) && Equals(other.Name, Name);
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != typeof(TableName)) return false;
			return Equals((TableName) obj);
		}

		public override int GetHashCode()
		{
			unchecked
			{
				return ((Schema != null ? Schema.GetHashCode() : 0)*397) ^ (Name != null ? Name.GetHashCode() : 0);
			}
		}
	}

	class TableInfo
	{
		public readonly TableName Name;
		public readonly IList<ColumnInfo> Columns = new List<ColumnInfo>();
		public KeyInfo PrimaryKey;
		public bool HasIdentity;

		public TableInfo (TableName name)
		{
			Name = name;
		}

		public int ColumnIndex (string name)
		{
			for (int i = 0; i < Columns.Count; ++i)
				if (Columns[i].Name == name)
					return i;
			return -1;
		}

		public ColumnInfo Column (int index)
		{
			return Columns[index];
		}
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
		public readonly IList<ColumnInfo> Columns = new List<ColumnInfo>();

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

			try
			{
				return new Copier(source, dest, overwriteExistingDatabase).Run();
			}
			finally
			{
				sw.Stop();
				Console.WriteLine("\n\nElapsed time = {0:0.000} seconds", sw.Elapsed.TotalSeconds);
			}
		}

		private static int ShowUsage ()
		{
			Console.WriteLine("Usage: CopyDb fromServer;fromDb[;usr;pwd] toServer;toDb[;usr;pwd]");
			Console.WriteLine("\t-f\tForce overwriting existing destination database");
			return 1;
		}
	}
}
