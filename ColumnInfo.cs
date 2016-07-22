using System;
using System.Data;

namespace CopyDb
{
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
}