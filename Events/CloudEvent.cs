namespace WSDemo_1
{
    public class CloudEvent
    {
        public string id { get; set; }
        public string source { get; set; }
        public string specversion { get; set; }

        public string type { get; set; }
        public string subject { get; set; }
    }
}