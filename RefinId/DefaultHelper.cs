using System;
using RefinId.InformationSchema;

namespace RefinId
{
	/// <summary>
	/// Simplifies operations for default settings.
	/// </summary>
	public class DefaultHelper
	{
		private readonly string _connectionString;
		private readonly string _dbProviderName;
		private readonly string _configurationTableName;
		private readonly IUniqueKeysProvider _uniqueKeysProvider;
		private readonly Lazy<DefaultLongIdProvider> _provider;

		/// <summary>
		/// Store parameters for future use.
		/// </summary>
		public DefaultHelper(string connectionString, string dbProviderName, string configurationTableName = null, IUniqueKeysProvider uniqueKeysProvider = null)
		{
			_connectionString = connectionString;
			_dbProviderName = dbProviderName;
			_configurationTableName = configurationTableName;
			_uniqueKeysProvider = uniqueKeysProvider;

			_provider = new Lazy<DefaultLongIdProvider>(() => new DefaultLongIdProvider(GetStorage()));
		}

		private ILongIdStorage GetStorage()
		{
			return new DbLongIdStorage(_connectionString, _dbProviderName, _configurationTableName);
		}

		/// <summary>
		/// Returns <see cref="DefaultLongIdProvider"/> singleton (creates if needed).
		/// </summary>
		public ILongIdProvider GetProvider()
		{
			return _provider.Value;
		}

		/// <summary>
		/// Creates <see cref="DefaultLongIdInstaller"/> and pass parameters to <see cref="DefaultLongIdInstaller.Install"/>.
		/// </summary>
		public void Install(byte shard, byte reserved, bool useUniqueIfPrimaryKeyNotMatch, params Table[] tables)
		{
			var installer = new DefaultLongIdInstaller(_connectionString, _uniqueKeysProvider ?? new UniqueKeysProvider(),
				_dbProviderName, _configurationTableName);
			installer.Install(shard, reserved, useUniqueIfPrimaryKeyNotMatch, tables);
		}
	}
}