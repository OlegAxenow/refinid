using System.Collections.Generic;
using System.Data.Common;

namespace RefinId.InformationSchema
{
	/// <summary>
	///     Provides information about primary keys.
	/// </summary>
	public interface IUniqueKeysProvider
	{
		/// <summary>
		///     Returns <see cref="UniqueKey" /> instances for all unique and primary key constraints for current database.
		/// </summary>
		/// <param name="command"> <see cref="DbCommand" /> with open connection to use for constraints retrieving.</param>
		IEnumerable<UniqueKey> GetUniqueKeys(DbCommand command);
	}
}