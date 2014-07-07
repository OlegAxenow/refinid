﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace RefinId
{
	/// <summary>
	///     Default, thread-safe implementation of <see cref="ILongIdFactory" />.
	///     NB: If you add new type, factory should be recreated
	///     (it is the price of lock-free reading inside this class).
	/// </summary>
	public class DefaultLongIdFactory : ILongIdFactory
	{
		/// <summary>
		///     Stores last values.
		/// </summary>
		private readonly Dictionary<short, IdWrapper> _lastValues;

		/// <summary>
		///     <see cref="ILongIdStorage" /> to get or save last values.
		/// </summary>
		private readonly ILongIdStorage _storage;

		/// <summary>
		///     Initializes factory with <paramref name="storage" />.
		/// </summary>
		public DefaultLongIdFactory(ILongIdStorage storage)
		{
			if (storage == null) throw new ArgumentNullException("storage");
			List<long> values = SafeGetLastValues(storage);
			_storage = storage;

			_lastValues = new Dictionary<short, IdWrapper>(values.Count);

			for (int i = values.Count - 1; i >= 0; i--)
			{
				_lastValues[((LongId)values[i]).Type] = new IdWrapper(values[i]);
			}
		}

		/// <summary>
		///     <see cref="ILongIdFactory.Create" /> implementation.
		/// </summary>
		public long Create(short type)
		{
			return Interlocked.Increment(ref _lastValues[type].Id);
		}

		/// <summary>
		///     <see cref="ILongIdFactory.FlushToStorage" /> implementation.
		/// </summary>
		public void FlushToStorage()
		{
			_storage.SaveLastValues(_lastValues.Values.Select(x => x.Id));
		}

		private static List<long> SafeGetLastValues(ILongIdStorage storage)
		{
			if (storage == null) throw new ArgumentNullException("storage");

			List<long> values = storage.GetLastValues();

			if (values.Count > short.MaxValue)
				throw new InvalidOperationException(
					string.Format("Length of avaiable types {0} greater than {1}.", values.Count, short.MaxValue));
			return values;
		}

		/// <summary>
		///     Wraps <see cref="long" /> to alow to use <see cref="Interlocked" />
		///     for <see cref="Dictionary{TKey,TValue}" /> values.
		/// </summary>
		private class IdWrapper
		{
			public long Id;

			public IdWrapper(long id)
			{
				Id = id;
			}
		}
	}
}