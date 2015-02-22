using System.Collections.Generic;

namespace RefinId
{
	/// <summary>
	///     Storage to retrieve persisted <see cref="LongId" /> values.
	/// </summary>
	/// <remarks>
	///     Supports two modes:
	///     <list type="number">
	///         <item>
	///             <description>
	///                 Stores last values immediately with <see cref="SaveLastValue" /> and
	///                 retrieves after restart with default <see cref="GetLastValues" /> parameter.
	///             </description>
	///         </item>
	///         <item>
	///             <description>
	///                 Stores last values periodically with <see cref="SaveLastValues" /> and
	///                 retrieves after restart with <see cref="GetLastValues" /> parameter depending on situation
	///                 (<b>true</b> for maximum durability, <b>false</b> when expect that no new objects added).
	///             </description>
	///         </item>
	///     </list>
	/// </remarks>
	public interface ILongIdStorage
	{
		/// <summary>
		///     Gets all last values (one for each <see cref="LongId.Type" />)
		///     from storage (e.g. after restart or adding new type).
		/// </summary>
		/// <param name="requestFromRealTables">
		///     Whether to request identifiers from real tables via primary keys
		///     instead of specific storage.
		/// </param>
		/// <remarks>
		///     Uses <see cref="long" /> to simplify implementation.
		///     <see cref="long" /> can be implicitly converted to <see cref="LongId" /> and vice versa.
		/// </remarks>
		/// <returns>
		///     <see cref="List{T}" /> with identifiers used to simplify getting capacity and to reduce
		///     <see cref="List{T}.ToArray" /> conversion.
		/// </returns>
		List<long> GetLastValues(bool requestFromRealTables = false);

		/// <summary>
		///     Saves last <see cref="LongId" /> values to storage (for <see cref="LongId.Type" />).
		/// </summary>
		/// <remarks>
		///     Uses <see cref="long" /> to simplify implementation.
		///     <see cref="long" /> can be implicitly converted to <see cref="LongId" /> and vice versa.
		/// </remarks>
		/// <param name="values"> Required values.</param>
		/// <param name="removeUnusedRows"> Whether to remove absent entries inside <see cref="SaveLastValues" />.</param>
		void SaveLastValues(IEnumerable<long> values, bool removeUnusedRows = true);

		/// <summary>
		///     Saves last <see cref="LongId" /> value to storage (for <see cref="LongId.Type" />).
		/// </summary>
		/// <remarks>
		///     Uses <see cref="long" /> to simplify implementation.
		///     <see cref="long" /> can be implicitly converted to <see cref="LongId" /> and vice versa.
		/// </remarks>
		void SaveLastValue(long value);

		/// <summary>
		/// Gets current <see cref="RefinId.TableCommandBuilder"/>.
		/// </summary>
		TableCommandBuilder Builder { get; }
	}
}