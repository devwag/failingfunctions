using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using System;

namespace FailingFunctions
{
    public static class httptrigger
    {
        [FunctionName("httptrigger")]
        public static async Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Function, "get", Route = null)]HttpRequestMessage req, TraceWriter log)
        {
            try
            {
                log.Info("C# HTTP trigger function processed a request.");

                // parse query parameter
                string name = req.GetQueryNameValuePairs()
                    .FirstOrDefault(q => string.Compare(q.Key, "name", true) == 0)
                    .Value;

                string throwerror = req.GetQueryNameValuePairs()
                    .FirstOrDefault(q => string.Compare(q.Key, "throwerror", true) == 0)
                    .Value;
                if (throwerror != null)
                    throw new Exception("You asked the function to fail");

                // Get request body
                dynamic data = await req.Content.ReadAsAsync<object>();

                // Set name to query string or body data
                name = name ?? data?.name;

                return name == null
                    ? req.CreateResponse(HttpStatusCode.BadRequest, "Please pass a name on the query string or in the request body")
                    : req.CreateResponse(HttpStatusCode.OK, "Hello there " + name);
            }
            catch (Exception ex)
            {
                return req.CreateResponse(HttpStatusCode.InternalServerError, "And error occured processing your request: " + ex.Message);
            }
        }
    }
}
