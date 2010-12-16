using System.Data;
using System.Data.SqlClient;

namespace CopyDb
{
	/// <summary>
	/// Utility methods to create a Command object
	/// </summary>
	public class Command
	{
		/// <summary>
		/// Create a Command object with a specified Connection, Transaction, CommandType, CommandText, and Parameters
		/// </summary>
		public static SqlCommand Create (SqlConnection cn, SqlTransaction trans, CommandType commandtype, string commandtext, params object[] namesandvalues)
		{
			SqlCommand cmd = new SqlCommand(commandtext, cn, trans);
			cmd.CommandType = commandtype;

			int i = 0;
			while (i < namesandvalues.Length)
			{
				string name = namesandvalues[i++].ToString();
				object value = namesandvalues[i++];
				SqlParameter parm = new SqlParameter(name, value);
				if (i < namesandvalues.Length && namesandvalues[i] is SqlDbType)
					parm.SqlDbType = (SqlDbType) namesandvalues[i++];
				if (i < namesandvalues.Length && namesandvalues[i] is ParameterDirection)
					parm.Direction = (ParameterDirection) namesandvalues[i++];
				cmd.Parameters.Add(parm);
			}
			System.Diagnostics.Debug.Assert(i == namesandvalues.Length, "Command.Create(): Arguments must be name-value pairs.");
			return cmd;
		}

		/// <summary>
		/// Create a Command object with a specified Connection, CommandType, CommandText, and Parameters.
		/// No Transaction.
		/// </summary>
		public static SqlCommand Create (SqlConnection cn, CommandType commandtype, string commandtext, params object[] namesandvalues)
		{
			return Create(cn, null, commandtype, commandtext, namesandvalues);
		}

		/// <summary>
		/// Create a Command object with a specified CommandType, CommandText, and Parameters.
		/// No Connection or Transaction.
		/// </summary>
		public static SqlCommand Create (CommandType commandtype, string commandtext, params object[] namesandvalues)
		{
			return Create(null, null, commandtype, commandtext, namesandvalues);
		}

		public class Sql
		{
			/// <summary>
			/// Create a SQL-statement Command object with a specified Connection, Transaction, SQL, and Parameters
			/// </summary>
			public static SqlCommand Create (SqlConnection cn, SqlTransaction trans, string sql, params object[] namesandvalues)
			{
				return Command.Create(cn, trans, CommandType.Text, sql, namesandvalues);
			}
			/// <summary>
			/// Create a SQL-statement Command object with a specified Connection, SQL, and Parameters
			/// No Transaction.
			/// </summary>
			public static SqlCommand Create (SqlConnection cn, string sql, params object[] namesandvalues)
			{
				return Create(cn, null, sql, namesandvalues);
			}
			/// <summary>
			/// Create a SQL-statement Command object with a specified SQL and Parameters
			/// No Connection or Transaction.
			/// </summary>
			public static SqlCommand Create (string sql, params object[] namesandvalues)
			{
				return Create(null, null, sql, namesandvalues);
			}
		}

		public class Sp
		{
			/// <summary>
			/// Create a Stored-Procedure Command object with a specified Connection, Transaction, SP Name, and Parameters
			/// </summary>
			public static SqlCommand Create (SqlConnection cn, SqlTransaction trans, string spname, params object[] namesandvalues)
			{
				return Command.Create(cn, trans, CommandType.StoredProcedure, spname, namesandvalues);
			}
			/// <summary>
			/// Create a Stored-Procedure Command object with a specified Connection, SP Name, and Parameters
			/// No Transaction.
			/// </summary>
			public static SqlCommand Create (SqlConnection cn, string spname, params object[] namesandvalues)
			{
				return Create(cn, null, spname, namesandvalues);
			}
			/// <summary>
			/// Create a Stored-Procedure Command object with a specified SP Name and Parameters
			/// No Connection or Transaction.
			/// </summary>
			public static SqlCommand Create (string spname, params object[] namesandvalues)
			{
				return Create(null, null, spname, namesandvalues);
			}
		}
	}
}
