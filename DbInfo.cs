using System;
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
}