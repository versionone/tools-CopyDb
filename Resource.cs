using System;
using System.Reflection;
using System.IO;

namespace CopyDb
{
	public class Resource
	{
		public static string LoadString (string resourcename)
		{
			Assembly asm = Assembly.GetCallingAssembly();
			return LoadString(asm.GetName().Name + "." + resourcename, asm);
		}

		public static string LoadString (string resourcename, Assembly asm)
		{
			if (asm == null)
				throw (new System.ArgumentNullException("asm"));
			using (Stream resourcestream = asm.GetManifestResourceStream(resourcename))
			{
				if (resourcestream == null)
					throw new  System.Resources.MissingManifestResourceException(resourcename);
				using (StreamReader reader = new StreamReader(resourcestream))
					return reader.ReadToEnd();
			}
		}
		public static string LoadClassString (string name, Type classtype)
		{
			if (classtype == null)
				throw (new System.ArgumentNullException("classtype"));
			string resourcename = classtype.FullName + "." + name;
			Assembly asm = classtype.Assembly;
			return LoadString(resourcename, asm);
		}
	}
}
