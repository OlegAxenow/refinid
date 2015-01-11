using System.Collections.Generic;
using System.Data.Common;
using RefinId.InformationSchema;

namespace Refinid.Specs
{
	public class TestDbMetadataProvider : IDbMetadataProvider
	{
		private readonly UniqueKey[] _uniqueKeys;

		public TestDbMetadataProvider(params UniqueKey[] uniqueKeys)
		{
			_uniqueKeys = uniqueKeys;
		}

		public IEnumerable<UniqueKey> GetUniqueKeys(DbCommand command, string dataType)
		{
			return _uniqueKeys;
		}

		public bool TableExists(DbCommand command, string tableName)
		{
			return true;
		}

		public string GetParameterName(string invariantName)
		{
			return "@" + invariantName;
		}
	}
}