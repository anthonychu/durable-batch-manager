using System.Collections.Generic;

namespace Company.Function
{
    public class TaskItem
    {
        public string Id { get; set; }
        public string SomeText { get; set; }
    }

    public class TaskHierarchy
    {
        public string Id { get; set; }
        public string ComputeSetId { get; set; }
        public IList<TaskItem> TaskItems { get; set; }
    }
}