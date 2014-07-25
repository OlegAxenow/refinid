namespace RefinId
{
	/// <summary>
	/// Contains table's parameters for using by installers and storages.
	/// </summary>
	public class TableParameters
	{
		/// <summary>
		/// Dafault value for <see cref="Schema"/>, if not specified in constructor.
		/// </summary>
		private const string DefaultSchema = "dbo";

		/// <summary>
		/// Creates instance with specified parameters.
		/// </summary>
		/// <param name="typeId"> Type's identifier for current table (corresponds to <see cref="LongId.Type"/>.</param>
		/// <param name="name"> Unquoted table's name.</param>
		/// <param name="schema"> Unquoted table's schema (<see cref="DefaultSchema"/>, by default).</param>
		public TableParameters(short typeId, string name, string schema = null)
		{
			TypeId = typeId;
			Name = name;
			Schema = schema ?? DefaultSchema;
		}

		/// <summary>
		/// Type's identifier for current table (corresponds to <see cref="LongId.Type"/>.
		/// </summary>
		public short TypeId { get; private set; }

		/// <summary>
		/// Unquoted table's name.
		/// </summary>
		public string Name { get; private set; }

		/// <summary>
		/// Unquoted table's schema ("dbo", by default).
		/// </summary>
		public string Schema { get; private set; }

		// TODO: PK and initializing strategy?
	}
}