/*
 Based on CrossThreadTestRunner by Peter Provost
 * http://www.peterprovost.org/blog/2004/11/03/Using-CrossThreadTestRunner/
 */

using System;
using System.Reflection;
using System.Security.Permissions;
using System.Threading;
using System.Threading.Tasks;

namespace RefinId.Specs
{
	/// <summary>
	///     Runs test in parallel threads and properly handle asserts.
	/// </summary>
	public class MultiThreadTestRunner
	{
		private readonly ThreadStart _userDelegate;
		private Exception _lastException;

		public MultiThreadTestRunner(ThreadStart userDelegate)
		{
			_userDelegate = userDelegate;
		}

		/// <summary>
		///     Runs <see cref="_userDelegate" /> in <paramref name="times" /> parallel threads.
		/// </summary>
		/// <param name="times">Desired thread count.</param>
		/// <param name="millisecondsAverageDelay">Average delay between each thread start time.</param>
		/// <param name="millisecondsTimeout">Time limit for all threads.</param>
		public void Run(int times = 1, int millisecondsAverageDelay = 10, int millisecondsTimeout = 10000)
		{
			CheckThreadCountAndSetThreadPool(times);
			var tasks = new Task[times];
			var random = new Random();
			for (int i = 0; i < times; i++)
			{
				tasks[i] = Task.Factory.StartNew(MultiThreadedWorker);
				if (i < times - 1)
					Thread.Sleep(Convert.ToInt32((0.5 + random.NextDouble()) * millisecondsAverageDelay));
			}

			Task.WaitAll(tasks, millisecondsTimeout);

			if (_lastException != null) ThrowExceptionPreservingStack(_lastException);
		}

		private static void CheckThreadCountAndSetThreadPool(int threadCount)
		{
			if (threadCount < 0 || threadCount > 100) throw new ArgumentOutOfRangeException("threadCount");

			int workerThreads, completionPortThreads;
			ThreadPool.GetMaxThreads(out workerThreads, out completionPortThreads);

			if (workerThreads < threadCount)
				ThreadPool.SetMaxThreads(threadCount + 1, completionPortThreads);
		}

		[ReflectionPermission(SecurityAction.Demand)]
		private void ThrowExceptionPreservingStack(Exception exception)
		{
			FieldInfo remoteStackTraceString = typeof(Exception).GetField("_remoteStackTraceString",
				BindingFlags.Instance | BindingFlags.NonPublic);
			if (remoteStackTraceString != null)
				remoteStackTraceString.SetValue(exception, exception.StackTrace + Environment.NewLine);
			throw exception;
		}

		private void MultiThreadedWorker()
		{
			try
			{
				_userDelegate.Invoke();
			}
			catch (Exception e)
			{
				_lastException = e;
			}
		}
	}
}