using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace Company.Function
{
    public static class Orchestrators
    {
        [FunctionName(nameof(TaskOrchestrator))]
        public static async Task TaskOrchestrator(
            [OrchestrationTrigger]IDurableOrchestrationContext context,
            ILogger log)
        {
            log = context.CreateReplaySafeLogger(log);
            var taskHierarchy = context.GetInput<TaskHierarchy>();
            var computeSet = context.CreateEntityProxy<IComputeSet>(new EntityId(nameof(ComputeSet), taskHierarchy.ComputeSetId));

            log.LogWarning("*** {eventName}: {taskId} on compute set {computeSetId}", "TasksQueued", taskHierarchy.Id, taskHierarchy.ComputeSetId);
            await computeSet.QueueTasksForOrchestration(context.InstanceId);
            
            await context.WaitForExternalEvent("TasksCompleted");
            log.LogWarning("*** {eventName}: {taskId} on compute set {computeSetId}", "TasksCompleted", taskHierarchy.Id, taskHierarchy.ComputeSetId);
        }

        [FunctionName(nameof(TaskExecutionOrchestrator))]
        public static async Task TaskExecutionOrchestrator(
            [OrchestrationTrigger]IDurableOrchestrationContext context,
            ILogger log)
        {
            log = context.CreateReplaySafeLogger(log);
            var request = context.GetInput<TaskExecutionRequest>();
            var taskHierarchy = request.Tasks;

            log.LogWarning("*** {eventName}: {taskId} on compute set {computeSetId}", "TasksStarting", taskHierarchy.Id, taskHierarchy.ComputeSetId);
            
            foreach (var taskItem in taskHierarchy.TaskItems)
            {
                var message = await context.CallActivityAsync<string>("ExecuteTask", taskItem);
            }

            //log.LogWarning("*** {eventName}: compute set {computeSetId}", "SignalTasksCompleted", taskHierarchy.ComputeSetId);
            var computeSet = context.CreateEntityProxy<IComputeSet>(new EntityId(nameof(ComputeSet), taskHierarchy.ComputeSetId));
            await computeSet.SignalTasksCompleted(request);
        }

        [FunctionName(nameof(ExecuteTask))]
        public static async Task<string> ExecuteTask([ActivityTrigger] TaskItem taskItem, ILogger log)
        {
            // pretend to do something for 5 seconds
            await Task.Delay(5000);
            return $"*** Completed item {taskItem.Id}";
        }

        [FunctionName(nameof(HttpStart))]
        public static async Task<List<string>> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")]HttpRequestMessage req,
            [DurableClient]IDurableOrchestrationClient starter,
            ILogger log)
        {
            var taskHierarchyCount = 30;
            var computeSetCount = 5;
            var tasksPerHierarchy = 3;
            var instanceIds = new List<string>();
            var rand = new Random();

            for (var i = 0; i < taskHierarchyCount; i++)
            {
                var taskHierarchy = new TaskHierarchy
                {
                    Id = $"{i}",
                    ComputeSetId = rand.Next(computeSetCount).ToString(),
                    TaskItems = Enumerable.Range(0, tasksPerHierarchy).Select(j => new TaskItem
                    {
                        Id = $"{i}-{j}",
                        SomeText = "Some text"
                    }).ToList()
                };
                string instanceId = await starter.StartNewAsync(nameof(TaskOrchestrator), taskHierarchy);
                //log.LogWarning($"*** Started orchestration with ID = '{instanceId}'.");
                instanceIds.Add(instanceId);
            }

            return instanceIds;
        }
    }
}