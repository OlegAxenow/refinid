using System.Collections.Generic;

namespace RefinId
{
	/// <summary>
	/// Storage to retrieve persisted <see cref="LongId"/> values.
	/// </summary>
	public interface ILongIdStorage
	{
		/// <summary>
		/// Gets all last values (one for each <see cref="LongId.Type"/>) 
		/// from storage (e.g. after restart or adding new type).
		/// </summary>
		/// <remarks> Uses <see cref="long"/> to simplify implementation.
		/// <see cref="long"/> can be implicitly converted to <see cref="LongId"/> and vice versa.
		/// </remarks>
		/// <returns><see cref="List{T}"/> with identifiers used to simplify getting capacity and to reduce <see cref="List{T}.ToArray"/> conversion.</returns>
		List<long> GetLastValues();

		/// <summary>
		/// Saves last <see cref="LongId"/> values to storage (for <see cref="LongId.Type"/>).
		/// </summary>
		/// <remarks> Uses <see cref="long"/> to simplify implementation.
		/// <see cref="long"/> can be implicitly converted to <see cref="LongId"/> and vice versa.</remarks>
		void SaveLastValues(IEnumerable<long> values);
	}
}