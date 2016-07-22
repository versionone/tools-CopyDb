namespace CopyDb
{
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
}