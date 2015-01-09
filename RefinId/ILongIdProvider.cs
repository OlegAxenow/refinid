namespace RefinId
{
	/// <summary>
	///     Provides methods to create <see cref="LongId" /> and flush all data to storage.
	/// </summary>
	public interface ILongIdProvider
	{
		/// <summary>
		///     Creates new unique sequential <see cref="LongId" /> with <see cref="LongId.Type" />,
		///     specified by <paramref name="type" />.
		/// </summary>
		long Create(short type);

		/// <summary>
		///     Flushes current state (last created identifiers) to storage.
		/// </summary>
		void FlushToStorage();
	}
}