using System.Collections.Generic;

namespace CopyDb
{
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
}