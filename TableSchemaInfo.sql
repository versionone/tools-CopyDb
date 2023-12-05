SELECT
	TABLE_SCHEMA SchemaName,
	TABLE_NAME TableName,
	COLUMN_NAME ColumnName,
	DATA_TYPE Type,
	Size = CASE
		WHEN DATA_TYPE IN ('binary','varbinary','char','nchar','varchar','nvarchar')--,'text','ntext','image')
			THEN CHARACTER_MAXIMUM_LENGTH
	END,
	[Precision] = CASE
		WHEN DATA_TYPE IN ('decimal','numeric')
			THEN cast(NUMERIC_PRECISION as int)
	END,
	Scale = CASE
		WHEN DATA_TYPE IN ('decimal','numeric')
			THEN cast(NUMERIC_SCALE as int)
	END,
	IsNullable = CASE Upper(COLUMNS.IS_NULLABLE) WHEN 'NO' THEN cast(0 as bit) ELSE cast(1 as bit) END,
	IsIdentity = CASE COLUMNPROPERTY(OBJECT_ID(TABLE_NAME), COLUMN_NAME, 'IsIdentity') WHEN 1 THEN cast(1 as bit) ELSE cast(0 as bit) END,
	IdentitySeed = CASE COLUMNPROPERTY(OBJECT_ID(TABLE_NAME), COLUMN_NAME, 'IsIdentity') WHEN 1 THEN cast(IDENT_SEED(TABLE_NAME) as int) END,
	IdentityIncrement = CASE COLUMNPROPERTY(OBJECT_ID(TABLE_NAME), COLUMN_NAME, 'IsIdentity') WHEN 1 THEN cast(IDENT_INCR(TABLE_NAME) as int) END,
	Calculation = com.definition,
	cast(ORDINAL_POSITION as int) Position,
	Collation = COLUMNS.COLLATION_NAME
FROM INFORMATION_SCHEMA.COLUMNS
JOIN sys.objects o on o.name=TABLE_NAME and o.schema_id=SCHEMA_ID(TABLE_SCHEMA)
left join sys.extended_properties e on class=1 and e.major_id=o.object_id and e.minor_id=0 and e.name=N'microsoft_database_tools_support'
LEFT JOIN sys.computed_columns com on com.object_id=OBJECT_ID(TABLE_NAME) AND com.name=COLUMN_NAME
where o.type='U' AND ObjectProperty(o.object_id, N'IsMSShipped')=0 AND (o.parent_object_id=0 OR ObjectProperty(o.parent_object_id, N'IsMSShipped')=0)
	and isnull(e.value, 0)<>1
--where TABLE_NAME=@tablename
ORDER BY TABLE_NAME, ORDINAL_POSITION

select
	SchemaName = schema_name(schema_id),
	TableName = o.name,
	ConstraintName = i.name,
	IsClustered = cast(case when i.type_desc='CLUSTERED' then 1 else 0 end as bit),
	ColumnName = col_name(c.object_id, c.column_id),
	IsDescending = c.is_descending_key,
	Position = c.key_ordinal,
	IsPrimaryKey = is_primary_key,
	IsUnique = is_unique,
	IsConstraint = is_primary_key | is_unique_constraint,
	IgnoreDupKey = ignore_dup_key
from sys.indexes i
join sys.objects o on o.object_id=i.object_id
join sys.index_columns c on c.object_id=i.object_id and c.index_id=i.index_id
left join sys.extended_properties e on class=1 and e.major_id=o.object_id and e.minor_id=0 and e.name=N'microsoft_database_tools_support'
where i.type_desc='CLUSTERED'
	and o.type='U' AND ObjectProperty(o.object_id, N'IsMSShipped')=0 AND (o.parent_object_id=0 OR ObjectProperty(o.parent_object_id, N'IsMSShipped')=0)
	and isnull(e.value, 0)<>1
order by o.name, i.name, c.key_ordinal
