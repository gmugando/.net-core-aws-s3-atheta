using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.Threading.Tasks;
using Newtonsoft;
using Newtonsoft.Json;
using System.IO;
using Amazon.Athena;
using Amazon.Athena.Model;
using System.Collections.Generic;

namespace S3CRUDOperation
{
     class Program
    {
        private const string AWS_ACCESS_KEY = "AKIAJGJ262SKQ4FLIIEQ";
        private const string AWS_SECRET_KEY = "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";
        private const string BUCKET_NAME = "test-bucket-kalam";
        private const string S3_KEY = "s3_key";
       public static async Task Main(string[] args)
        {


            var credentials = new BasicAWSCredentials(AWS_ACCESS_KEY, AWS_SECRET_KEY);
            AmazonS3Client client = new AmazonS3Client(credentials, RegionEndpoint.USEast2);






                  //Create Bucket If Not Exists
                  await CreateBucketAsync(client);

            //Write Books
            Console.WriteLine("Do you want to Save Book with Key Y/N...............................");
            var result = Console.ReadKey();
            Console.WriteLine("");
            if (result.KeyChar.ToString().ToLower() == "y")
            {
                Console.WriteLine("Please Enter is a key to Save Book with Dummy Data...............................");
                var bookKey = Console.ReadLine();
                await WriteBookOnS3Async(client, bookKey+".json");
                Console.WriteLine("Book Data has been saved Sucessfully");

            }


            //Serach by Key
            Console.WriteLine("Do you want to Search Book with Key Y/N...............................");
            Console.WriteLine("Search By Book Id:1");
            Console.WriteLine("Search By Book Description :2");
            var searchbykeyresult = Console.ReadLine();
            Console.WriteLine("");
            if (searchbykeyresult.ToString().ToLower() =="1")
            {
                
                Console.WriteLine("Please Enter a Value...............................");
                var skey = Console.ReadLine();
                Console.WriteLine("Searching Book on S3 Bucket .......");
                await AWSAthenaSearchById(skey);

//                await SearchS3ObjectAsync(client,skey);
            }
            else
            {
                Console.WriteLine("Please Enter a Value...............................");
                var skey = Console.ReadLine();
                Console.WriteLine("Searching Book on S3 Bucket .......");
                await AWSAthenaSearchByDescription(skey);

            }




            //Console.WriteLine("Do you want to Search Book with Book ID Y/N...............................");

            //var serachByBookresult = Console.ReadKey();
            //Console.WriteLine("");
            //if (serachByBookresult.KeyChar.ToString().ToLower() == "y")
            //{

            //    Console.WriteLine("Please Enter a Book ID...............................");
            //    var skey = Console.ReadLine();
            //    Console.WriteLine("Searching Book in S3 Bucket with Book Id.......");
            //    await SearchFromBucketWithBookIdAsync(client, skey);
            //}


            Console.WriteLine("Do you want to Delete Book with Key Y/N...............................");
            result = Console.ReadKey();
            Console.WriteLine("");
            if (result.KeyChar.ToString().ToLower() == "y")
            {
                Console.WriteLine("Please Enter Key...............................");
                var skey = Console.ReadLine();
                Console.WriteLine("Deleting Book on S3 Bucket .......");
                await DeletS3Object(client,skey);

            }
            Console.WriteLine("Book has deleted SCuessfully");
        }

        private static async Task CreateBucketAsync(AmazonS3Client client)
        {
            Console.Out.WriteLine("Checking S3 bucket with name " + BUCKET_NAME);

            ListBucketsResponse response = await client.ListBucketsAsync();

            bool found = false;
            foreach (S3Bucket bucket in response.Buckets)
            {
                if (bucket.BucketName == BUCKET_NAME)
                {
                    Console.Out.WriteLine("Bucket already Exists...............................");
                    found = true;
                    break;
                }
            }

            if (found == false)
            {
                Console.Out.WriteLine("Bucket is Creating...............................");

                PutBucketRequest request = new PutBucketRequest();
                request.BucketName = BUCKET_NAME;

                await client.PutBucketAsync(request);

                Console.Out.WriteLine("Bucket has been Created S3 bucket with name " + BUCKET_NAME);
            }
        }
        private static async Task WriteBookOnS3Async(AmazonS3Client client,string key)
        {

            // Create a PutObject request
            PutObjectRequest request = new PutObjectRequest
            {
                BucketName = BUCKET_NAME,
                Key = key,
                ContentBody = GetBookObject()
            };

            // Put object
            PutObjectResponse response = await client.PutObjectAsync(request);


        }
        public static async Task<Book> SearchS3ObjectAsync(AmazonS3Client client, string key)
        {
            GetObjectRequest request = new GetObjectRequest
            {
                BucketName = BUCKET_NAME,
                Key = key
            };

            GetObjectResponse response = await client.GetObjectAsync(request);

            StreamReader reader = new StreamReader(response.ResponseStream);

            string content = reader.ReadToEnd();
            if (!string.IsNullOrWhiteSpace(content))
            {
                //Console.WriteLine("Following Content Found.......");
                //Console.WriteLine(content);
                return JsonConvert.DeserializeObject<Book>(content);
            }
            else
                Console.WriteLine("No Data Found");
            return null;
        }
        private static async Task DeletS3Object(AmazonS3Client client,string key)
        {
            // Create a DeleteObject request
            DeleteObjectRequest request = new DeleteObjectRequest
            {
                BucketName = BUCKET_NAME,
                Key = key
            };

            // Issue request
           var result = await client.DeleteObjectAsync(request);

                Console.WriteLine("Book Has been removed Sucessfully");


        }

        public static async Task SearchFromBucketWithBookIdAsync(AmazonS3Client client, string bookId)
        {
            ListObjectsRequest req = new ListObjectsRequest {
                BucketName = BUCKET_NAME
            };

            ListObjectsResponse res = await client.ListObjectsAsync(req);
            foreach (S3Object obj in res.S3Objects)
            {
                var result = await SearchS3ObjectAsync(client, obj.Key).ConfigureAwait(true);

                if (result.bookid.ToString()== bookId.ToString())
                {
                    Console.WriteLine("Object Found with "+ bookId);
                    Console.WriteLine(JsonConvert.SerializeObject(result));
                }
            }
        }

        public static async Task AWSAthenaSearchById(string searchkey)
        {
            using (var client = new AmazonAthenaClient(AWS_ACCESS_KEY, AWS_SECRET_KEY, Amazon.RegionEndpoint.USEast2))
            {
                QueryExecutionContext qContext = new QueryExecutionContext();
                qContext.Database = "bookdb";
                ResultConfiguration resConf = new ResultConfiguration();
                resConf.OutputLocation = "s3://test-bucket-kalam/";

                Console.WriteLine("Created Athena Client");


                /* Execute a simple query on a table */
                StartQueryExecutionRequest qReq = new StartQueryExecutionRequest()
                {
                    QueryString = "SELECT * FROM book where bookid='" + searchkey + "'",
                    QueryExecutionContext = qContext,
                    ResultConfiguration = resConf
                };

                try
                {
                    /* Executes the query in an async manner */
                    StartQueryExecutionResponse qRes = await client.StartQueryExecutionAsync(qReq);
                    /* Call internal method to parse the results and return a list of key/value dictionaries */
                    List<Dictionary<String, String>> items = await getQueryExecution(client, qRes.QueryExecutionId);
                    foreach (var item in items)
                    {
                        foreach (KeyValuePair<String, String> pair in item)
                        {
                            Console.WriteLine("Col: {0}", pair.Key);
                            Console.WriteLine("Val: {0}", pair.Value);
                        }
                    }
                }
                catch (InvalidRequestException e)
                {
                    Console.WriteLine("Run Error: {0}", e.Message);
                }

            }
        }

        public static async Task AWSAthenaSearchByDescription(string searchkey)
        {
            using (var client = new AmazonAthenaClient(AWS_ACCESS_KEY, AWS_SECRET_KEY, Amazon.RegionEndpoint.USEast2))
            {
                QueryExecutionContext qContext = new QueryExecutionContext();
                qContext.Database = "bookdb";
                ResultConfiguration resConf = new ResultConfiguration();
                resConf.OutputLocation = "s3://test-bucket-kalam/";

                Console.WriteLine("Created Athena Client");


                /* Execute a simple query on a table */
                StartQueryExecutionRequest qReq = new StartQueryExecutionRequest()
                {
                    QueryString = "SELECT * FROM book where bookdescription like '%" + searchkey + "%'",
                    QueryExecutionContext = qContext,
                    ResultConfiguration = resConf
                };

                try
                {
                    /* Executes the query in an async manner */
                    StartQueryExecutionResponse qRes = await client.StartQueryExecutionAsync(qReq);
                    /* Call internal method to parse the results and return a list of key/value dictionaries */
                    List<Dictionary<String, String>> items = await getQueryExecution(client, qRes.QueryExecutionId);
                    foreach (var item in items)
                    {
                        foreach (KeyValuePair<String, String> pair in item)
                        {
                            Console.WriteLine("Col: {0}", pair.Key);
                            Console.WriteLine("Val: {0}", pair.Value);
                        }
                    }
                }
                catch (InvalidRequestException e)
                {
                    Console.WriteLine("Run Error: {0}", e.Message);
                }

            }
        }



        async static Task<List<Dictionary<String, String>>> getQueryExecution(IAmazonAthena client, String id)
        {
            List<Dictionary<String, String>> items = new List<Dictionary<String, String>>();
            GetQueryExecutionResponse results = null;
            QueryExecution q = null;
            /* Declare query execution request object */
            GetQueryExecutionRequest qReq = new GetQueryExecutionRequest()
            {
                QueryExecutionId = id
            };
            /* Poll API to determine when the query completed */
            do
            {
                try
                {
                    results = await client.GetQueryExecutionAsync(qReq);
                    q = results.QueryExecution;
                    Console.WriteLine("Status: {0}... {1}", q.Status.State, q.Status.StateChangeReason);

                    await Task.Delay(5000); //Wait for 5sec before polling again
                }
                catch (InvalidRequestException e)
                {
                    Console.WriteLine("GetQueryExec Error: {0}", e.Message);
                }
            } while (q.Status.State == "RUNNING" || q.Status.State == "QUEUED");

            Console.WriteLine("Data Scanned for {0}: {1} Bytes", id, q.Statistics.DataScannedInBytes);

            /* Declare query results request object */
            GetQueryResultsRequest resReq = new GetQueryResultsRequest()
            {
                QueryExecutionId = id,
                MaxResults = 10
            };

            GetQueryResultsResponse resResp = null;
            /* Page through results and request additional pages if available */
            do
            {
                resResp = await client.GetQueryResultsAsync(resReq);
                /* Loop over result set and create a dictionary with column name for key and data for value */
                foreach (Row row in resResp.ResultSet.Rows)
                {
                    Dictionary<String, String> dict = new Dictionary<String, String>();
                    for (var i = 0; i < resResp.ResultSet.ResultSetMetadata.ColumnInfo.Count; i++)
                    {
                        dict.Add(resResp.ResultSet.ResultSetMetadata.ColumnInfo[i].Name, row.Data[i].VarCharValue);
                    }
                    items.Add(dict);
                }

                if (resResp.NextToken != null)
                {
                    resReq.NextToken = resResp.NextToken;
                }
            } while (resResp.NextToken != null);

            /* Return List of dictionary per row containing column name and value */
            return items;
        }


        async static Task run(IAmazonAthena client, QueryExecutionContext qContext, ResultConfiguration resConf, string searchkey)
        {
            /* Execute a simple query on a table */
            StartQueryExecutionRequest qReq = new StartQueryExecutionRequest()
            {
                QueryString = "SELECT * FROM book where bookid='" + searchkey + "'",
                QueryExecutionContext = qContext,
                ResultConfiguration = resConf
            };

            try
            {
                /* Executes the query in an async manner */
                StartQueryExecutionResponse qRes = await client.StartQueryExecutionAsync(qReq);
                /* Call internal method to parse the results and return a list of key/value dictionaries */
                List<Dictionary<String, String>> items = await getQueryExecution(client, qRes.QueryExecutionId);
                foreach (var item in items)
                {
                    foreach (KeyValuePair<String, String> pair in item)
                    {
                        Console.WriteLine("Col: {0}", pair.Key);
                        Console.WriteLine("Val: {0}", pair.Value);
                    }
                }
            }
            catch (InvalidRequestException e)
            {
                Console.WriteLine("Run Error: {0}", e.Message);
            }
        }













        //Get Dummy Books Data
        private static string GetBookObject()
        {
            Book book = new Book();
            book.bookid = "12000";
            book.bookname = "Test Book Name";
            book.bookdescription = "This is a Book two";
            return JsonConvert.SerializeObject(book);
        }
    }
}
