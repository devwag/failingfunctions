    #r "Newtonsoft.Json"
#r "Microsoft.WindowsAzure.Storage"
using Microsoft.WindowsAzure.Storage.Table;

    using System;
    using Newtonsoft.Json;

    public static void Run(string myEventHubMessage, ICollector<string> outputQueueItem, ICollector<TableMessage> outputTable, TraceWriter log)
    {
        

            //outputQueueItem.Add(myEventHubMessage);
        log.Info($"C# Event Hub trigger function processed a message: {myEventHubMessage}");
        var e = JsonConvert.DeserializeObject<EventData>(myEventHubMessage);
        outputTable.Add(
                new Person() { 
                    PartitionKey = "Test", 
                    RowKey = e.messageid,
                    Count = "1",
                    Message = myEventHubMessage
                    }
                );
        if(e.code == "500"){
            //this is a missing temp
            log.Info("Unrecoverable error");
        }
        if (e.code == "400") {
            e.code = "200"; 
            var res = JsonConvert.SerializeObject(e);
            log.Info ($"retry with payload {res}");
            // retry

            outputQueueItem.Add(res);
        }
        if (e.code == "200") {
            float temps = 0;
            if (float.TryParse(e.temperature, out temps)){
                log.Info("The temp is a number");
                if(temps > 100){
                    log.Info($"Out of range: {temps}");
                }
                else if (temps == -1){
                    log.Info($"Error Temp: {temps}");
                }
                else{
                    log.Info($"Temp is in range: {temps}");
                }
            }
            else{
                log.Info($"Cannot parse temperature {e.temperature}");
            }
        }
    }

    public class EventData
    {
        public string messageid{ get; set; }
        public string deviceid{ get; set; }
        public string temperature { get; set; }
        public string humidity { get; set; }
        public string code { get; set; }
    }

    public class TableMessage
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string Count { get; set; }
        public string Message { get; set; }
    }