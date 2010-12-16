using System;
using System.Data;
using System.Globalization;
using System.Text;

namespace CopyDb
{
	internal sealed class SqlTypeMeta
	{
		private SqlTypeMeta () {}

		public delegate string LiteralFormatter (object value);

		private static readonly LiteralFormatter[] _formatters =
		{
			//BigInt
			new LiteralFormatter(BigIntegerFormatter),
			//Binary
			new LiteralFormatter(BinaryFormatter),
			//Bit
			new LiteralFormatter(BooleanFormatter),
			//Char
			new LiteralFormatter(AnsiStringFormatter),
			//DateTime
			new LiteralFormatter(DateTimeFormatter),
			//Decimal
			new LiteralFormatter(RealFormatter),
			//Float
			new LiteralFormatter(RealFormatter),
			//Image
			new LiteralFormatter(BinaryFormatter),
			//Int
			new LiteralFormatter(IntegerFormatter),
			//Money
			new LiteralFormatter(RealFormatter),
			//NChar
			new LiteralFormatter(UnicodeStringFormatter),
			//NText
			new LiteralFormatter(UnicodeStringFormatter),
			//NVarChar
			new LiteralFormatter(UnicodeStringFormatter),
			//Real
			new LiteralFormatter(RealFormatter),
			//UniqueIdentifier
			new LiteralFormatter(GuidFormatter),
			//SmallDateTime
			new LiteralFormatter(DateTimeFormatter),
			//SmallInt
			new LiteralFormatter(IntegerFormatter),
			//SmallMoney
			new LiteralFormatter(RealFormatter),
			//Text
			new LiteralFormatter(AnsiStringFormatter),
			//Timestamp
			new LiteralFormatter(BinaryFormatter),
			//TinyInt
			new LiteralFormatter(IntegerFormatter),
			//VarBinary
			new LiteralFormatter(BinaryFormatter),
			//VarChar
			new LiteralFormatter(AnsiStringFormatter),
			//Variant
			new LiteralFormatter(VariantFormatter),
		};

		#region Formatters
		private static string AnsiStringFormatter (object value)
		{
			if (value == DBNull.Value) return "null";
			return FormatAnsiString(value.ToString());
		}

		private static string UnicodeStringFormatter (object value)
		{
			if (value == DBNull.Value) return "null";
			return FormatUnicodeString(value.ToString());
		}

		private static string BooleanFormatter (object value)
		{
			if (value == DBNull.Value) return "null";
			return FormatBoolean((bool) value);
		}

		private static string DateTimeFormatter (object value)
		{
			if (value == DBNull.Value) return "null";
			return FormatDateTime((DateTime) value);
		}

		private static string BinaryFormatter (object value)
		{
			if (value == DBNull.Value) return "null";
			return FormatBinary((byte[]) value);
		}

		private static string GuidFormatter (object value)
		{
			if (value == DBNull.Value) return "null";
			return FormatGuid((Guid) value);
		}

		private static string IntegerFormatter (object value)
		{
			if (value == DBNull.Value) return "null";
			return FormatInteger((IFormattable) value);
		}

		private static string BigIntegerFormatter (object value)
		{
			if (value == DBNull.Value) return "null";
			if (value is long)
				return FormatBigInteger((long) value);
			if (value is ulong)
				return FormatBigInteger((ulong) value);
			return FormatBigInteger((IFormattable) value);
		}

		private static string RealFormatter (object value)
		{
			if (value == DBNull.Value) return "null";
			return FormatReal((IFormattable) value);
		}

		private static string VariantFormatter (object value)
		{
			if (value == DBNull.Value) return "null";
			return FormatVariant(value);
		}
		#endregion

		#region Format Functions
		public static string FormatAnsiString (string value)
		{
			return "'" + value.Replace("'", "''") + "'";
		}

		public static string FormatUnicodeString (string value)
		{
			return "N'" + value.Replace("'", "''") + "'";
		}

		public static string FormatBoolean (bool value)
		{
			return (value)? "1": "0";
		}

		public static string FormatDateTime (DateTime value)
		{
			return "'" + value.ToString("yyyy-MM-ddTHH:mm:ss.fff", DateTimeFormatInfo.InvariantInfo) + "'";
		}

		public static string FormatBinary (byte[] value)
		{
			return "0x" + Hex.Encode(value);
		}

		public static string FormatGuid (Guid value)
		{
			return value.ToString("D");
		}

		public static string FormatInteger (byte value)
		{
			return value.ToString("G", NumberFormatInfo.InvariantInfo);
		}

		public static string FormatInteger (sbyte value)
		{
			return value.ToString("G", NumberFormatInfo.InvariantInfo);
		}

		public static string FormatInteger (int value)
		{
			return value.ToString("G", NumberFormatInfo.InvariantInfo);
		}

		public static string FormatInteger (uint value)
		{
			return value.ToString("G", NumberFormatInfo.InvariantInfo);
		}

		public static string FormatInteger (short value)
		{
			return value.ToString("G", NumberFormatInfo.InvariantInfo);
		}

		public static string FormatInteger (ushort value)
		{
			return value.ToString("G", NumberFormatInfo.InvariantInfo);
		}

		public static string FormatInteger (IFormattable value)
		{
			return value.ToString("G", NumberFormatInfo.InvariantInfo);
		}

		public static string FormatBigInteger (long value)
		{
			if (value >= -2147483648L && value <= 2147483647L)
				return value.ToString("G", NumberFormatInfo.InvariantInfo);
			return "cast(" + value.ToString("G", NumberFormatInfo.InvariantInfo) + " as bigint)";
		}

		public static string FormatBigInteger (ulong value)
		{
			if (value <= 2147483647UL)
				return value.ToString("G", NumberFormatInfo.InvariantInfo);
			return "cast(" + value.ToString("G", NumberFormatInfo.InvariantInfo) + " as bigint)";
		}

		public static string FormatBigInteger (IFormattable value)
		{
			return "cast(" + value.ToString("G", NumberFormatInfo.InvariantInfo) + " as bigint)";
		}

		public static string FormatReal (float value)
		{
			return value.ToString("r", NumberFormatInfo.InvariantInfo);
		}

		public static string FormatReal (double value)
		{
			return value.ToString("r", NumberFormatInfo.InvariantInfo);
		}

		public static string FormatReal (decimal value)
		{
			return value.ToString("r", NumberFormatInfo.InvariantInfo);
		}

		public static string FormatReal (IFormattable value)
		{
			return value.ToString("r", NumberFormatInfo.InvariantInfo);
		}

		public static string FormatVariant (object value)
		{
			if (value is string) return FormatUnicodeString((string) value);
			if (value is bool) return FormatBoolean((bool) value);
			if (value is DateTime) return FormatDateTime((DateTime) value);
			if (value is byte[]) return FormatBinary((byte[]) value);
			if (value is Guid) return FormatGuid((Guid) value);
			if (value is byte || value is short || value is int || value is long || value is sbyte || value is ushort || value is uint || value is ulong)
				return FormatInteger((IFormattable) value);
			if (value is float || value is double || value is decimal)
				return FormatReal((IFormattable) value);
			return "(unsupported type: " + value.GetType().ToString() + "=" + value.ToString() + ")";
		}
		#endregion


		public static string Format (object value, SqlDbType type)
		{
			return _formatters[ (int) type ] (value);
		}
	}
}