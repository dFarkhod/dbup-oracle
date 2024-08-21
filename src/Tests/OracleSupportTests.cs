using System.Threading.Tasks;
using DbUp.Tests.Common;
using DbUp.Tests.Common.RecordingDb;
using Shouldly;
using VerifyXunit;
using Xunit;
using DbUp.Oracle;

namespace DbUp.Tests.Support.Oracle
{
    [UsesVerify]
    public class OracleSupportTests
    {
        [Fact]
        public Task CanHandleDelimiter()
        {
            var logger = new CaptureLogsLogger();
            var recordingDbConnection = new RecordingDbConnection(logger, "schemaversions");
            recordingDbConnection.SetupRunScripts();
            var upgrader = DeployChanges.To
                .OracleDatabaseWithDefaultDelimiter(string.Empty)  // Switch to Oracle database
                .OverrideConnectionFactory(recordingDbConnection)
                .LogTo(logger)
                .WithScript("Script0003", @"
BEGIN
    EXECUTE IMMEDIATE 'DROP PROCEDURE testSproc';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -4043 THEN
            RAISE;
        END IF;
END;

BEGIN
    EXECUTE IMMEDIATE '
        CREATE OR REPLACE PROCEDURE testSproc(
            ssn IN VARCHAR2
        ) IS
        BEGIN
            FOR rec IN (SELECT id FROM customer WHERE ssn = ssn) LOOP
                -- Your logic here
            END LOOP;
        END;';
END;")
                .Build();

            var result = upgrader.PerformUpgrade();

            result.Successful.ShouldBe(true);
            return Verifier.Verify(logger.Log, VerifyHelper.GetVerifySettings());
        }
    }

}
