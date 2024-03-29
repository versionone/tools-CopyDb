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
		public readonly bool IgnoreDupKey;
		public readonly byte FillFactor;
		public readonly bool PadIndex;
		public readonly bool AllowRowLocks;
		public readonly bool AllowPageLocks;
		public readonly IList<ColumnInfo> Columns = new List<ColumnInfo>();

		public KeyInfo (string name, bool isclustered, bool isPrimaryKey, bool isUnique, bool isConstraint, bool ignoreDupKey, byte fillFactor, bool padIndex, bool allowRowLocks, bool allowPageLocks)
		{
			Name = name;
			IsClustered = isclustered;
			IsPrimaryKey = isPrimaryKey;
			IsUnique = isUnique;
			IsConstraint = isConstraint;
			IgnoreDupKey = ignoreDupKey;
			FillFactor = fillFactor;
			PadIndex = padIndex;
			AllowRowLocks = allowRowLocks;
			AllowPageLocks = allowPageLocks;
		}

		public KeyInfo (object name, object isclustered, object isPrimaryKey, object isUnique, object isConstraint, object ignoreDupKey, object fillFactor, object padIndex, object allowRowLocks, object allowPageLocks)
			: this(
				(string) name,
				(bool) isclustered,
				(bool)isPrimaryKey,
				(bool)isUnique,
				(bool)isConstraint,
				(bool)ignoreDupKey,
				(byte)fillFactor,
				(bool)padIndex,
				(bool)allowRowLocks,
				(bool)allowPageLocks
				)
		{}
	}
}
