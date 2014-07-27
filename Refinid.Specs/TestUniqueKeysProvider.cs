using System.Collections.Generic;
using System.Data.Common;
using RefinId.InformationSchema;

namespace Refinid.Specs
{
	public class TestUniqueKeysProvider : IUniqueKeysProvider
	{
		private readonly UniqueKey[] _uniqueKeys;

		public TestUniqueKeysProvider(params UniqueKey[] uniqueKeys)
		{
			_uniqueKeys = uniqueKeys;
		}

		public IEnumerable<UniqueKey> GetUniqueKeys(DbCommand command)
		{
			return _uniqueKeys;
		}
	}
}