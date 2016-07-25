using System.Collections.Generic;

namespace CopyDb
{
	class KeyInfo
	{
		public readonly string Name;
		public readonly bool IsClustered;
		public readonly bool IsPrimaryKey;
		public readonly bool IsUnique;
		public readonly bool IsConstraint;
		public readonly IList<ColumnInfo> Columns = new List<ColumnInfo>();

		public KeyInfo (string name, bool isclustered, bool isPrimaryKey, bool isUnique, bool isConstraint)
		{
			Name = name;
			IsClustered = isclustered;
			IsPrimaryKey = isPrimaryKey;
			IsUnique = isUnique;
			IsConstraint = isConstraint;
		}

		public KeyInfo (object name, object isclustered, object isPrimaryKey, object isUnique, object isConstraint)
			: this(
				(string) name,
				(bool) isclustered,
				(bool)isPrimaryKey,
				(bool)isUnique,
				(bool)isConstraint
				)
		{}
	}
}