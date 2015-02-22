using System;
using System.Collections.Generic;

namespace RefinId.Specs
{
	public class TestStorage : ILongIdStorage
	{
		private List<long> _values;

		public TestStorage(params long[] values)
		{
			_values = new List<long>(values);
		}

		public List<long> Values
		{
			get { return _values; }
		}

		public List<long> GetLastValues(bool requestFromRealTables = false)
		{
			return new List<long>(_values);
		}

		public void SaveLastValues(IEnumerable<long> values,
			bool removeUnusedRows = true)
		{
			_values = new List<long>(values);
		}

		public void SaveLastValue(long value)
		{
			SaveLastValues(new[] { value });
		}

		public TableCommandBuilder Builder
		{
			get { throw new NotImplementedException(); }
		}
	}
}