using System.Diagnostics.CodeAnalysis;

namespace RefinId
{
	/// <summary>
	///     Contains schema and table name to use in different classes with table information.
	/// </summary>
	public interface ISchemaAndTable
	{
		/// <summary>
		///     Unquoted table's schema (e.g. "dbo").
		/// </summary>
		[SuppressMessage("StyleCop.CSharp.DocumentationRules", "SA1650:ElementDocumentationMustBeSpelledCorrectly", Justification = "Reviewed. Suppression is OK here.")]
		string Schema { get; }

		/// <summary>
		///     Unquoted table's name.
		/// </summary>
		string TableName { get; }
	}
}