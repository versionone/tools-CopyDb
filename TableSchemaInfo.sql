SELECT
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
	IsNullable = CASE Upper(IS_NULLABLE) WHEN 'NO' THEN cast(0 as bit) ELSE cast(1 as bit) END,
	IsIdentity = CASE COLUMNPROPERTY(OBJECT_ID(TABLE_NAME), COLUMN_NAME, 'IsIdentity') WHEN 1 THEN cast(1 as bit) ELSE cast(0 as bit) END,
	IdentitySeed = CASE COLUMNPROPERTY(OBJECT_ID(TABLE_NAME), COLUMN_NAME, 'IsIdentity') WHEN 1 THEN cast(IDENT_SEED(TABLE_NAME) as int) END,
	IdentityIncrement = CASE COLUMNPROPERTY(OBJECT_ID(TABLE_NAME), COLUMN_NAME, 'IsIdentity') WHEN 1 THEN cast(IDENT_INCR(TABLE_NAME) as int) END,
	Calculation = com.text,
	cast(ORDINAL_POSITION as int) Position,
	Collation = COLLATION_NAME
FROM INFORMATION_SCHEMA.COLUMNS 
JOIN sysobjects o on o.name=TABLE_NAME
LEFT JOIN syscomments com on com.id=OBJECT_ID(TABLE_NAME) AND com.number=ORDINAL_POSITION
where o.xtype='U' AND ObjectProperty(o.id, N'IsMSShipped')=0 AND (o.parent_obj=0 OR ObjectProperty(o.parent_obj, N'IsMSShipped')=0)
--where TABLE_NAME=@tablename
ORDER BY TABLE_NAME, ORDINAL_POSITION

SELECT 
	TableName = kcu.TABLE_NAME,
	ConstraintName = kcu.CONSTRAINT_NAME,
	IsClustered = cast(INDEXPROPERTY(OBJECT_ID(kcu.TABLE_NAME),kcu.CONSTRAINT_NAME , 'IsClustered') as bit),
	ColumnName = kcu.COLUMN_NAME,
	IsDescending = cast(INDEXKEY_PROPERTY(OBJECT_ID(kcu.TABLE_NAME), INDEXPROPERTY(OBJECT_ID(kcu.TABLE_NAME),kcu.CONSTRAINT_NAME,'IndexID'), ORDINAL_POSITION, 'IsDescending') as bit),
	kcu.ORDINAL_POSITION Position
FROM 
	INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
	JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc ON tc.CONSTRAINT_NAME=kcu.CONSTRAINT_NAME
WHERE 
	tc.CONSTRAINT_TYPE='PRIMARY KEY'
--	and kcu.TABLE_NAME=@tablename
ORDER BY kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
