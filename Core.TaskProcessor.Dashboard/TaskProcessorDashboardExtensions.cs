using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection;

namespace Core.TaskProcessor.Dashboard
{
    public class TaskProcessorDashboardOptions
    {
        public string Prefix { get; set; } = "/taskprocessor";
        public Func<HttpContext, Task<string>> TenantProvider { get; set; } = _ => Task.FromResult("core");
    }

    public static class TaskProcessorDashboardExtensions
    {
        public static void AddTaskProcessorDashboard(this IServiceCollection services, TaskProcessorDashboardOptions options)
        {
            services.AddSingleton(options);
        }

        public static void MapTaskProcessorDashboard(this WebApplication app)
        {
            var options = app.Services.GetRequiredService<TaskProcessorDashboardOptions>();

            app.MapGet($"{options.Prefix}/batches", async (ITaskProcessor proc) =>
            {
                return await proc.GetBatchesAsync("core", 0, 50);
            });

            app.MapGet($"{options.Prefix}/schedules", async (ITaskProcessor proc) =>
            {
                return await proc.GetSchedulesAsync("core", 0, 50);
            });

            app.MapGet($"{options.Prefix}/queues", async (ITaskProcessor proc) =>
            {
                return await proc.GetQueuesAsync();
            });//.RequireAuthorization("taskprocessor_admin");

            app.MapPost($"{options.Prefix}/batch/{{batchId}}/cancel", async (ITaskProcessor proc, [FromRoute] string batchId) =>
            {
                return await proc.CancelBatchAsync(batchId);
            });//.RequireAuthorization("taskprocessor_admin");

            app.MapPost($"{options.Prefix}/schedule/{{scheduleId}}/cancel", async (ITaskProcessor proc, [FromRoute] string scheduleId) =>
            {

                return await proc.CancelScheduleAsync(scheduleId, );
            });//.RequireAuthorization("taskprocessor_admin");

            app.MapPost($"{options.Prefix}/schedule/{{scheduleId}}/fire", async (ITaskProcessor proc, [FromRoute] string batchId) =>
            {
                return await proc.CancelBatchAsync(batchId);
            });//.RequireAuthorization("taskprocessor_admin");

            app.usesta
        }
    }
}
