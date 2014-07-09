namespace RefinId
{
	/// <summary>
	/// Contains parameters for using by installers and storages.
	/// </summary>
	public class TableParameters
	{
		/// <summary>
		/// Type's identifier for current table.
		/// </summary>
		public short TypeId { get; set; }

		/// <summary>
		/// Unquoted table's name.
		/// </summary>
		public string Name { get; set; }

		/// <summary>
		/// Unquoted table's schema (e.g. dbo).
		/// </summary>
		public string Schema { get; set; }

		// TODO: PK and initializing strategy?
	}
}