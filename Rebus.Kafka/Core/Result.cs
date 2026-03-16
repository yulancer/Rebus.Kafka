namespace Rebus.Kafka.Core
{
    internal class Result
    {
        internal bool Success { get; }
        internal bool Failure => !Success;
        internal string Reason { get; }

        internal static Result Ok() => new Result(true, null);
        internal static Result Fail(string reason) => new Result(false, reason);

        private Result(bool success, string reason)
        {
            Success = success;
            Reason = reason;
        }
    }
}
