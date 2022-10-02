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

        [HttpPost("enqueue")]
        public async Task<string> Enqueue()
        {
            var batchId = Guid.NewGuid().ToString("D");

            await _processor.EnqueueBatchAsync("default", "core", new List<TaskData>
            {
                new(),
                new(),
            }, continuations: new List<TaskData>
            {
                new()
                {
                    Topic = "continue"
                }
            }, "some tasks");

            return "ok";
        }

        [HttpGet("list")]
        public async Task<IEnumerable<BatchInfo>> Get()
        {
            return await _processor.GetBatchesAsync("core").ConfigureAwait(false);
        }
    }
}