using System.Collections.Generic;
using RefinId;

namespace Refinid.Specs
{
	public class TestStorage : ILongIdStorage
	{
		private List<long> _values;

		public TestStorage(params long[] values)
		{
			_values = new List<long>(values);
		}

		public List<long> GetLastValues()
		{
			return new List<long>(_values);
		}

		public void SaveLastValues(IEnumerable<long> values)
		{
			_values = new List<long>(values);
		}

		public List<long> Values
		{
			get { return _values; }
		}
	}
}