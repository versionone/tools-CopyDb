namespace System.Text
{
	public sealed class Hex
	{
		private static readonly char[] hexValues = new char[] {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
		private static readonly byte[] nibbleValues = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 0, 0, 0, 0, 0, 0, 10, 11, 12, 13, 14, 15};

		private Hex()
		{
		}

		private static byte DecodeHexDigit (char c)
		{
			return nibbleValues[c - '0'];
		}

		private static char EncodeHexNibble (byte value)
		{
			return hexValues[value];
		}

		public static byte[] Decode (string hexString)
		{
			return Decode(hexString, 0, hexString.Length);
		}

		public static unsafe byte[] Decode (string hexString, int start, int count)
		{
			byte[] byteArray = new byte[count >> 1];
			if (count > 0)
			{
				fixed (char* hexBegin = hexString)
					fixed (byte* byteBegin = byteArray)
					{
						byte* b = byteBegin;
						char* c = hexBegin + start;
						char* hexEnd = c + count;
						while (c < hexEnd)
						{
							int hi = Hex.DecodeHexDigit( *c++ );
							int lo = Hex.DecodeHexDigit( *c++ );
							*b++ = (byte) ( (hi<<4) | lo );
						}
					}
			}
			return byteArray;
		}

		public static string Encode (byte[] byteArray)
		{
			return Encode(byteArray, 0, byteArray.Length);
		}

		public static unsafe string Encode (byte[] byteArray, int start, int count)
		{
			char[] hexString = new char[count << 1];
			if (count > 0)
			{
				fixed (byte* byteBegin = byteArray)
					fixed (char* hexBegin = hexString)
					{
						char* c = hexBegin;
						byte* b = byteBegin + start;
						byte* byteEnd = b + count;
						while (b < byteEnd)
						{
							byte hi = (byte) ((*b & 0xF0) >> 4);
							*c++ = EncodeHexNibble(hi);
							byte lo = (byte) (*b & 0x0F);
							*c++ = EncodeHexNibble(lo);
							++b;
						}
					}
			}
			return new string(hexString);
		}
	}
}
