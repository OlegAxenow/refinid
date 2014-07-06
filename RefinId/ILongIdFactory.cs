namespace RefinId
{
	/// <summary>
	/// Provides factory to create <see cref="LongId"/>.
	/// </summary>
	public interface ILongIdFactory
	{
		/// <summary>
		/// Creates new unique sequential <see cref="LongId"/> with <see cref="LongId.Type"/>, 
		/// specified by <paramref name="type"/>.
		/// </summary>
		long Create(short type);

		/// <summary>
		/// Flushes current state (last created identifiers) to storage.
		/// </summary>
		void FlushToStorage();
	}
}