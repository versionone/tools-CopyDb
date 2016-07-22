using System;

namespace CopyDb
{
	class Global
	{
		[STAThread]
		static int Main (string[] args)
		{
			Stopwatch sw = new Stopwatch();
			sw.Start();

			bool overwriteExistingDatabase = false;
			DbInfo source = null;
			DbInfo dest = null;

			var positional = 0;
			foreach (var arg in args)
			{
				if (arg[0] == '-' || arg[0] == '/')
				{
					if (arg.Length < 2)
						return ShowUsage();
					switch (arg[1])
					{
						case '?':
						case 'h':
							return ShowUsage();
						case 'f':
							overwriteExistingDatabase = true;
							break;
						default:
							return ShowUsage();
					}
				}
				else
				{
					switch (++positional)
					{
						case 1:
							source = new DbInfo(arg);
							break;
						case 2:
							dest = new DbInfo(arg);
							break;
					}
				}
			}
			if (positional < 2)
				return ShowUsage();

			try
			{
				return new Copier(source, dest, overwriteExistingDatabase).Run();
			}
			finally
			{
				sw.Stop();
				Console.WriteLine("\n\nElapsed time = {0:0.000} seconds", sw.Elapsed.TotalSeconds);
			}
		}

		private static int ShowUsage ()
		{
			Console.WriteLine("Usage: CopyDb fromServer;fromDb[;usr;pwd] toServer;toDb[;usr;pwd]");
			Console.WriteLine("\t-f\tForce overwriting existing destination database");
			return 1;
		}
	}
}
