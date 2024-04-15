using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.FileProviders;

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

            app.MapGet($"{options.Prefix}/api/batches", async (ITaskProcessor proc) =>
            {
                return await proc.GetBatchesAsync("core", 0, 50);
            });

            app.MapGet($"{options.Prefix}/api/schedules", async (ITaskProcessor proc) =>
            {
                return await proc.GetSchedulesAsync("core", 0, 50);
            });

            app.MapGet($"{options.Prefix}/api/queues", async (ITaskProcessor proc) =>
            {
                return await proc.GetQueuesAsync();
            });//.RequireAuthorization("taskprocessor_admin");

            app.MapPost($"{options.Prefix}/api/batch/{{batchId}}/cancel", async (ITaskProcessor proc, [FromRoute] string batchId) =>
            {
                return await proc.CancelBatchAsync(batchId);
            });//.RequireAuthorization("taskprocessor_admin");

            app.MapPost($"{options.Prefix}/api/schedule/{{scheduleId}}/cancel", 
                async (ITaskProcessor proc, HttpContext ctx, [FromRoute] string scheduleId) =>
            {
                var tenant = await options.TenantProvider.Invoke(ctx);
                return await proc.CancelScheduleAsync(scheduleId, tenant);
            });//.RequireAuthorization("taskprocessor_admin");

            app.MapPost($"{options.Prefix}/api/schedule/{{scheduleId}}/fire", 
                async (ITaskProcessor proc, [FromRoute] string scheduleId) =>
            {
                return await proc.TriggerScheduleAsync(scheduleId);
            });//.RequireAuthorization("taskprocessor_admin");

            app.Use(async (context, next) =>
            {
                if (!Path.HasExtension(context.Request.Path.Value)
                    && context.Request.Path.Value!.StartsWith(options.Prefix))
                {
                    context.Request.Path = $"{options.Prefix}/index.html";
                    context.Response.Headers.Add("cache-control", "no-cache, no-store");
                    context.Response.Headers.Add("expires", "-1");
                }

                await next();
            });

            app.UseStaticFiles(new StaticFileOptions
            {
                FileProvider = new EmbeddedFileProvider(typeof(TaskProcessorDashboardExtensions).Assembly, "Core.TaskProcessor.Dashboard.static"),
                RequestPath = options.Prefix
            });
        }
    }
}
