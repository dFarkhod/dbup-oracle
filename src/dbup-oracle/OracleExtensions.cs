using System;
using System.Data;
using System.Linq;
using DbUp.Builder;
using DbUp.Engine.Output;
using DbUp.Engine.Transactions;
using Oracle.ManagedDataAccess.Client;

namespace DbUp.Oracle
{
#pragma warning disable IDE0060 // Remove unused parameter - The "SupportedDatabases" parameter is never used.
    public static class OracleExtensions
    {
        /// <summary>
        /// Create an upgrader for Oracle databases.
        /// </summary>
        /// <param name="supported">Fluent helper type.</param>
        /// <param name="connectionString">The connection string.</param>
        /// <returns>
        /// A builder for a database upgrader designed for Oracle databases.
        /// </returns>
        /// <remarks>
        /// This method is obsolete. One of these should be used instead:
        /// <list type="bullet">
        /// <item><see cref="OracleDatabaseWithDefaultDelimiter(SupportedDatabases, string)"/></item>
        /// <item><see cref="OracleDatabaseWithSemicolonDelimiter(SupportedDatabases, string)"/></item>
        /// <item><see cref="OracleDatabase(SupportedDatabases, string, char)"/></item>
        /// </list>
        /// </remarks>
        [Obsolete("Use OracleDatabaseWithDefaultDelimiter, OracleDatabaseWithSemicolonDelimiter or the OracleDatabase with the delimiter parameter instead, see https://github.com/DbUp/DbUp/pull/335")]
        public static UpgradeEngineBuilder OracleDatabase(this SupportedDatabases supported, string connectionString)
        {
            foreach (var pair in connectionString.Split(';').Select(s => s.Split('=')).Where(pair => pair.Length == 2).Where(pair => pair[0].ToLower() == "database"))
            {
                return OracleDatabase(new OracleConnectionManager(connectionString), pair[1]);
            }

            return OracleDatabase(new OracleConnectionManager(connectionString));
        }

        /// <summary>
        /// Create an upgrader for Oracle databases that uses the <c>/</c> character as the delimiter between statements.
        /// </summary>
        /// <param name="supported">Fluent helper type.</param>
        /// <param name="connectionString">The connection string.</param>
        /// <returns>
        /// A builder for a database upgrader designed for Oracle databases.
        /// </returns>
        public static UpgradeEngineBuilder OracleDatabaseWithDefaultDelimiter(this SupportedDatabases supported, string connectionString)
            => OracleDatabase(supported, connectionString, '/');

        /// <summary>
        /// Create an upgrader for Oracle databases that uses the <c>;</c> character as the delimiter between statements.
        /// </summary>
        /// <param name="supported">Fluent helper type.</param>
        /// <param name="connectionString">The connection string.</param>
        /// <returns>
        /// A builder for a database upgrader designed for Oracle databases.
        /// </returns>
        public static UpgradeEngineBuilder OracleDatabaseWithSemicolonDelimiter(this SupportedDatabases supported, string connectionString)
            => OracleDatabase(supported, connectionString, ';');

        /// <summary>
        /// Create an upgrader for Oracle databases.
        /// </summary>
        /// <param name="supported">Fluent helper type.</param>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="delimiter">Character to use as the delimiter between statements.</param>
        /// <returns>
        /// A builder for a database upgrader designed for Oracle databases.
        /// </returns>
        public static UpgradeEngineBuilder OracleDatabase(this SupportedDatabases supported, string connectionString, char delimiter)
        {
            foreach (var pair in connectionString.Split(';').Select(s => s.Split('=')).Where(pair => pair.Length == 2).Where(pair => pair[0].ToLower() == "database"))
            {
                return OracleDatabase(new OracleConnectionManager(connectionString, new OracleCommandSplitter(delimiter)), pair[1]);
            }

            return OracleDatabase(new OracleConnectionManager(connectionString, new OracleCommandSplitter(delimiter)));
        }

        /// <summary>
        /// Create an upgrader for Oracle databases.
        /// </summary>
        /// <param name="supported">Fluent helper type.</param>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="delimiter">Character to use as the delimiter between statements.</param>
        /// <returns>
        /// A builder for a database upgrader designed for Oracle databases.
        /// </returns>
        public static UpgradeEngineBuilder OracleDatabase(this SupportedDatabases supported, string connectionString, string schema, char delimiter)
        {
            return OracleDatabase(new OracleConnectionManager(connectionString, new OracleCommandSplitter(delimiter)), schema);
        }

        /// <summary>
        /// Creates an upgrader for Oracle databases.
        /// </summary>
        /// <param name="supported">Fluent helper type.</param>
        /// <param name="connectionString">Oracle database connection string.</param>
        /// <param name="schema">Which Oracle schema to check for changes</param>
        /// <returns>
        /// A builder for a database upgrader designed for Oracle databases.
        /// </returns>
        [Obsolete("Use the parameter that takes a delimiter instead, see https://github.com/DbUp/DbUp/pull/335")]
        public static UpgradeEngineBuilder OracleDatabase(this SupportedDatabases supported, string connectionString, string schema)
        {
            return OracleDatabase(new OracleConnectionManager(connectionString), schema);
        }

        /// <summary>
        /// Creates an upgrader for Oracle databases.
        /// </summary>
        /// <param name="supported">Fluent helper type.</param>
        /// <param name="connectionString">Oracle database connection string.</param>
        /// <param name="schema">Which Oracle schema to check for changes</param>
        /// <returns>
        /// A builder for a database upgrader designed for Oracle databases.
        /// </returns>
        /// <remarks>
        /// This method is obsolete. One of these should be used instead:
        /// <list type="bullet">
        /// <item><see cref="OracleDatabaseWithDefaultDelimiter(SupportedDatabases, string)"/></item>
        /// <item><see cref="OracleDatabaseWithSemicolonDelimiter(SupportedDatabases, string)"/></item>
        /// <item><see cref="OracleDatabase(SupportedDatabases, string, char)"/></item>
        /// </list>
        /// </remarks>
        [Obsolete("Use OracleDatabaseWithDefaultDelimiter, OracleDatabaseWithSemicolonDelimiter or the OracleDatabase with the delimiter parameter instead, see https://github.com/DbUp/DbUp/pull/335")]
        public static UpgradeEngineBuilder OracleDatabase(this SupportedDatabases supported, string connectionString, string schema, string delimiter)
        {
            return OracleDatabase(new OracleConnectionManager(connectionString), schema);
        }

        /// <summary>
        /// Creates an upgrader for Oracle databases.
        /// </summary>
        /// <param name="supported">Fluent helper type.</param>
        /// <param name="connectionManager">The <see cref="OracleConnectionManager"/> to be used during a database upgrade.</param>
        /// <returns>
        /// A builder for a database upgrader designed for Oracle databases.
        /// </returns>
        public static UpgradeEngineBuilder OracleDatabase(this SupportedDatabases supported, IConnectionManager connectionManager)
        {
            return OracleDatabase(connectionManager);
        }

        /// <summary>
        /// Creates an upgrader for Oracle databases.
        /// </summary>
        /// <param name="connectionManager">The <see cref="OracleConnectionManager"/> to be used during a database upgrade.</param>
        /// <returns>
        /// A builder for a database upgrader designed for Oracle databases.
        /// </returns>
        public static UpgradeEngineBuilder OracleDatabase(IConnectionManager connectionManager)
        {
            return OracleDatabase(connectionManager, null);
        }

        /// <summary>
        /// Creates an upgrader for Oracle databases.
        /// </summary>
        /// <param name="connectionManager">The <see cref="OracleConnectionManager"/> to be used during a database upgrade.</param>
        /// /// <param name="schema">Which Oracle schema to check for changes</param>
        /// <returns>
        /// A builder for a database upgrader designed for Oracle databases.
        /// </returns>
        public static UpgradeEngineBuilder OracleDatabase(IConnectionManager connectionManager, string schema)
        {
            var builder = new UpgradeEngineBuilder();
            builder.Configure(c => c.ConnectionManager = connectionManager);
            builder.Configure(c => c.ScriptExecutor = new OracleScriptExecutor(() => c.ConnectionManager, () => c.Log, null, () => c.VariablesEnabled, c.ScriptPreprocessors, () => c.Journal));
            builder.Configure(c => c.Journal = new OracleTableJournal(() => c.ConnectionManager, () => c.Log, schema, "schemaversions"));
            builder.WithPreprocessor(new OraclePreprocessor());
            return builder;
        }

#pragma warning restore IDE0060 // Remove unused parameter


        public static void OracleDatabase(this SupportedDatabasesForEnsureDatabase supported, string connectionString)
        {
            OracleDatabase(supported, connectionString, new ConsoleUpgradeLog());
        }

        public static void OracleDatabase(this SupportedDatabasesForEnsureDatabase supported, string connectionString, int commandTimeout)
        {
            OracleDatabase(supported, connectionString, new ConsoleUpgradeLog(), commandTimeout);
        }

        public static void OracleDatabase(this SupportedDatabasesForEnsureDatabase supported, string connectionString, IUpgradeLog logger, int timeout = -1)
        {
            GetOracleConnectionStringBuilder(connectionString, logger, out var adminConnectionString, out var schemaName);

            try
            {
                using (var connection = new OracleConnection(adminConnectionString))
                {
                    connection.Open();
                    if (SchemaExists(connection, schemaName))
                        return;
                }
            }
            catch (Exception e)
            {
                logger.WriteInformation(@"Schema not found on server with connection string in settings: {0}", e.Message);
            }

            using (var connection = new OracleConnection(adminConnectionString))
            {
                connection.Open();
                if (SchemaExists(connection, schemaName))
                    return;

                var sqlCommandText = string.Format
                        (
                            @"CREATE USER {0} IDENTIFIED BY password DEFAULT TABLESPACE users TEMPORARY TABLESPACE temp;",
                            schemaName
                        );

                using (var command = new OracleCommand(sqlCommandText, connection)
                {
                    CommandType = CommandType.Text
                })
                {
                    if (timeout >= 0)
                    {
                        command.CommandTimeout = timeout;
                    }

                    command.ExecuteNonQuery();
                }

                logger.WriteInformation(@"Created schema {0}", schemaName);
            }
        }

        static bool SchemaExists(OracleConnection connection, string schemaName)
        {
            var sqlCommandText = string.Format
            (
                $"SELECT username FROM dba_users WHERE username = '{schemaName.ToUpper()}';"
            );

            using (var command = new OracleCommand(sqlCommandText, connection)
            {
                CommandType = CommandType.Text
            })
            {
                var result = command.ExecuteScalar();
                return result != null;
            }
        }

        static void GetOracleConnectionStringBuilder(string connectionString, IUpgradeLog logger, out string adminConnectionString, out string schemaName)
        {
            if (string.IsNullOrEmpty(connectionString) || connectionString.Trim() == string.Empty)
                throw new ArgumentNullException(nameof(connectionString));

            if (logger == null)
                throw new ArgumentNullException(nameof(logger));

            var connectionStringBuilder = new OracleConnectionStringBuilder(connectionString);
            schemaName = connectionStringBuilder.UserID;

            if (string.IsNullOrEmpty(schemaName) || schemaName.Trim() == string.Empty)
                throw new InvalidOperationException("The connection string does not specify a schema name.");

            // Admin connection would generally have sysdba privileges or be connected to a common user in a multitenant environment.
            connectionStringBuilder.UserID = "SYS";
            connectionStringBuilder.Password = "your_admin_password"; // Needs to be a privileged user
            connectionStringBuilder.ConnectionString += ";DBA Privilege=SYSDBA";
            adminConnectionString = connectionStringBuilder.ConnectionString;
        }
    }
}
