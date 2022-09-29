using Microsoft.AspNetCore.Mvc;

namespace Core.TaskProcessor.SampleWebApi.Controllers
{
    [ApiController]
    [Route("api/batches")]
    public class BatchController : ControllerBase
    {
        private readonly ITaskProcessor _processor;

        public BatchController(ITaskProcessor processor)
        {
            _processor = processor;
        }

        [HttpPost]
        public async Task<string> Enqueue()
        {
            var batchId = Guid.NewGuid().ToString("D");

            await _processor.EnqueueBatchAsync("default", "core", batchId, new List<TaskData>
            {
                new()
                {
                    TaskId = Guid.NewGuid().ToString("D")
                },
                new()
                {
                    TaskId = Guid.NewGuid().ToString("D")
                },
            }, continuations: new List<TaskData>
            {
                new()
                {
                    Topic = "continue",
                    TaskId = Guid.NewGuid().ToString("D")
                }
            }, "some tasks");

            return "ok";
        }

        [HttpGet]
        public async Task<IEnumerable<BatchInfo>> Get()
        {
            return await _processor.GetBatchesAsync("core").ConfigureAwait(false);
        }
    }
}