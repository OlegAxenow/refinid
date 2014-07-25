using System.Data.SqlClient;
using RefinId;

namespace Refinid.Specs
{
	public class ConnectionHelper
	{
		public const string ConnectionString =
			"Server=(localdb)\\v11.0;Integrated Security=true;Database=" + TestDatabaseName;

		public const string TestDatabaseName = "RefineIdTest";

		public static void UseTestDatabase(SqlCommand command)
		{
			command.Run("USE " + TestDatabaseName);
		}

		public static SqlConnection CreateConnection()
		{
			return new SqlConnection(ConnectionString);
		}
	}
}