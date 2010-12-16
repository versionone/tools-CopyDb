using System;
using System.Runtime.InteropServices;

namespace CopyDb
{
	/// <summary>
	/// Provides a set of methods and properties that you can use to accurately measure elapsed time.
	/// Thanks Paul Welter (http://weblogs.asp.net/pwelter34).
	/// </summary>
	public struct Stopwatch
	{
		#region SafeNativeMethods

		[DllImport ("kernel32.dll")]
		private static extern bool QueryPerformanceFrequency (out long lpFrequency);

		[DllImport ("kernel32.dll")]
		private static extern bool QueryPerformanceCounter (out long lpPerformanceCount);

		#endregion //SafeNativeMethods

		#region Public Fields        

		/// <summary>Gets the frequency of the timer as the number of ticks per second. This field is read-only.</summary>
		public static readonly long Frequency;

		/// <summary>Indicates whether the timer is based on a high-resolution performance counter. This field is read-only.</summary>
		public static readonly bool IsHighResolution;

		#endregion //Public Fields

		#region Private Fields

		private long elapsed;
		private bool isRunning;
		private long startTimeStamp;

		private static readonly double tickFrequency;

		#endregion //Private Fields

		#region Constructors

		static Stopwatch ()
		{
			if (!QueryPerformanceFrequency(out Stopwatch.Frequency))
			{
				Stopwatch.IsHighResolution = false;
				Stopwatch.Frequency = TimeSpan.TicksPerSecond;
				Stopwatch.tickFrequency = 1;
			}
			else
			{
				Stopwatch.IsHighResolution = true;
				Stopwatch.tickFrequency = TimeSpan.TicksPerSecond;
				Stopwatch.tickFrequency /= ((double) Stopwatch.Frequency);
			}
		}

		#endregion //Constructors

		#region Private Methods

		private long GetElapsedDateTimeTicks ()
		{
			long ticks = this.GetRawElapsedTicks();
			if (Stopwatch.IsHighResolution)
			{
				double highTicks = ticks;
				highTicks *= Stopwatch.tickFrequency;
				return (long) highTicks;
			}
			return ticks;
		}

		private long GetRawElapsedTicks ()
		{
			long elapsedTimestamp = this.elapsed;
			if (this.isRunning)
			{
				long currentTimestamp = Stopwatch.GetTimestamp();
				long endTimestamp = currentTimestamp - this.startTimeStamp;
				elapsedTimestamp += endTimestamp;
			}
			return elapsedTimestamp;
		}

		#endregion //Private Methods

		#region Public Methods

		/// <summary>Gets the current number of ticks in the timer mechanism.</summary>
		/// <returns>A long integer representing the tick counter value of the underlying timer mechanism.</returns>
		public static long GetTimestamp ()
		{
			if (Stopwatch.IsHighResolution)
			{
				long ticks = 0;
				QueryPerformanceCounter(out ticks);
				return ticks;
			}
			return DateTime.UtcNow.Ticks;
		}

		/// <summary>Stops time interval measurement and resets the elapsed time to zero.</summary>
		public void Reset ()
		{
			this.elapsed = 0;
			this.isRunning = false;
			this.startTimeStamp = 0;
		}

		/// <summary>Starts, or resumes, measuring elapsed time for an interval.</summary>
		public void Start ()
		{
			if (!this.isRunning)
			{
				this.isRunning = true;
				this.startTimeStamp = Stopwatch.GetTimestamp();
			}
		}

		/// <summary>Initializes a new Stopwatch instance, sets the elapsed time property to zero, and starts measuring elapsed time.</summary>
		/// <returns>A Stopwatch that has just begun measuring elapsed time.</returns>
		public static Stopwatch StartNew ()
		{
			Stopwatch stopwatch = new Stopwatch();
			stopwatch.Start();
			return stopwatch;
		}

		/// <summary>Stops measuring elapsed time for an interval.</summary>
		public void Stop ()
		{
			long currentTimestamp = Stopwatch.GetTimestamp();
			if (this.isRunning)
			{
				long endTimestamp = currentTimestamp - this.startTimeStamp;
				this.elapsed += endTimestamp;
				this.isRunning = false;
			}
		}

		#endregion //Public Methods

		#region Public Properties

		/// <summary>Gets the total elapsed time measured by the current instance.</summary>
		/// <value>A read-only System.TimeSpan representing the total elapsed time measured by the current instance.</value>
		public TimeSpan Elapsed
		{
			get {return new TimeSpan(this.GetElapsedDateTimeTicks());}
		}

		/// <summary>Gets the total elapsed time measured by the current instance, in milliseconds.</summary>
		public long ElapsedMilliseconds
		{
			get {return (this.GetElapsedDateTimeTicks()/TimeSpan.TicksPerMillisecond);}
		}

		/// <summary>Gets the total elapsed time measured by the current instance, in timer ticks.</summary>
		public long ElapsedTicks
		{
			get {return this.GetRawElapsedTicks();}
		}

		/// <summary>Gets a value indicating whether the Stopwatch timer is running.</summary>
		public bool IsRunning
		{
			get {return this.isRunning;}
		}

		#endregion //Public Properties
	}
}