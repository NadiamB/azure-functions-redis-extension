using System.Collections.Generic;

namespace Microsoft.Azure.WebJobs.Extensions.Redis.Samples
{
    public record ListData
    (
        string id,
        List<string> value
    );
}
