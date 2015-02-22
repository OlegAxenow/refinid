using System.Collections.Generic;
using System.Data.Common;

namespace RefinId.Metadata
{
	/// <summary>
	///     Provides information about tables, primary and unique keys.
	/// </summary>
	/// <remarks> You can implement your own <see cref="IDbMetadataProvider"/> 
	/// e.g. to treat all BIGINT columns as unique in <see cref="GetUniqueKeys"/>.</remarks>
	public interface IDbMetadataProvider
	{
		/// <summary>
		///     Returns <see cref="UniqueKey" /> instances for all *eligible* unique and primary key constraints for current database.
		/// </summary>
		/// <param name="command"> <see cref="DbCommand" /> with open connection to use for constraints retrieving.</param>
		/// <param name="dataType"> Required parameter for column data type matching.</param>
		/// <remarks> "Eligible" means that constraint contains single column with specified data type.</remarks>
		IEnumerable<UniqueKey> GetUniqueKeys(DbCommand command, string dataType);

		/// <summary>
		/// Returns whether or not table with specified name exists.
		/// </summary>
		/// <param name="command"> <see cref="DbCommand" /> with open connection to use for constraints retrieving.</param>
		/// <param name="tableName"> Required parameter for table name matching.</param>
		bool TableExists(DbCommand command, string tableName);

		/// <summary>
		/// Returns correctly formatted parameter name for specific database (e.g. name -> @name).
		/// </summary>
		/// <remarks>
		/// <see cref="DbProviderFactories"/> does not provide necessary API for this.</remarks>
		string GetParameterName(string invariantName);
	}
}