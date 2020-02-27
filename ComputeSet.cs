using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;

namespace Company.Function
{
    public class ComputeSet : IComputeSet
    {
        public ComputeSetStatus Status { get; set; } = ComputeSetStatus.NotStarted;
        public TaskExecutionRequest CurrentRequest { get; set; }
        public Queue<string> InstanceIdQueue { get; set; } = new Queue<string>();

        private IDurableOrchestrationClient client;

        public ComputeSet(IDurableOrchestrationClient client)
        {
            this.client = client;
        }

        [FunctionName(nameof(ComputeSet))]
        public static Task Run(
                [EntityTrigger]IDurableEntityContext ctx,
                [DurableClient]IDurableOrchestrationClient client)
            => ctx.DispatchAsync<ComputeSet>(client);

        public Task QueueTasksForOrchestration(string taskOrchestratorInstanceId)
        {
            // TODO: if not running, call an orchestration to start compute set

            InstanceIdQueue.Enqueue(taskOrchestratorInstanceId);
            return RunNextTaskIfComputeIsAvailable();
        }

        public async Task SignalTasksCompleted(TaskExecutionRequest request)
        {
            if (request.TaskOrchestratorInstanceId != CurrentRequest.TaskOrchestratorInstanceId)
            {
                // this would be bad but should never happen
            }
            CurrentRequest = null;
            await client.RaiseEventAsync(request.TaskOrchestratorInstanceId, "TasksCompleted", null);
            await RunNextTaskIfComputeIsAvailable();
        }

        private async Task RunNextTaskIfComputeIsAvailable()
        {
            var computeIsAvailable = CurrentRequest == null;
            if (!computeIsAvailable) {
                return;
            }

            if (!InstanceIdQueue.TryDequeue(out var instanceId))
            {
                // no pending tasks
                return;
            }

            // get the task hierarchy from the input of the orchestrator
            // (so the entity only needs to keep a queue of orchestration ids)
            var taskOrchestrator = await client.GetStatusAsync(instanceId);
            var taskHierarchy = taskOrchestrator.Input.ToObject<TaskHierarchy>();

            CurrentRequest = new TaskExecutionRequest
            {
                TaskOrchestratorInstanceId = instanceId,
                Tasks = taskHierarchy
            };
            await client.StartNewAsync(nameof(Orchestrators.TaskExecutionOrchestrator), null, CurrentRequest);
        }
    }

    public interface IComputeSet
    {
        Task QueueTasksForOrchestration(string TaskOrchestratorInstanceId);
        Task SignalTasksCompleted(TaskExecutionRequest request);
    }

    public enum ComputeSetStatus
    {
        NotStarted,
        Starting,
        Running
    }

    public class TaskExecutionRequest
    {
        public string TaskOrchestratorInstanceId { get; set; }
        public TaskHierarchy Tasks { get; set; }
    }
}