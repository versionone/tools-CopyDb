using System.Collections.Generic;

namespace CopyDb
{
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
}