using Rebus.Kafka.Core;
using Xunit;

namespace Rebus.Kafka.Tests.Core
{
    public class ResultTests
    {
        [Fact]
        public void Ok_ReturnsSuccess()
        {
            var result = Result.Ok();
            Assert.True(result.Success);
            Assert.False(result.Failure);
            Assert.Null(result.Reason);
        }

        [Fact]
        public void Fail_ReturnsFailureWithReason()
        {
            const string reason = "test reason";
            var result = Result.Fail(reason);
            Assert.False(result.Success);
            Assert.True(result.Failure);
            Assert.Equal(reason, result.Reason);
        }
    }
}
