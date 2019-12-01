using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Model;
using CsvHelper;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace UpdateBins
{
    public class Function
    {
        IAmazonS3 S3Client { get; set; }
        private static readonly RegionEndpoint bucketRegion = RegionEndpoint.EUWest1;

        public Function()
        {
            S3Client = new AmazonS3Client();
        }

        public Function(IAmazonS3 s3Client)
        {
            this.S3Client = s3Client;
        }

        public async Task<string> FunctionHandler(S3Event evnt, ILambdaContext context)
        {
            var s3Event = evnt.Records?[0].S3;
            if (s3Event == null)
            {
                return null;
            }
            else
            {

                try
                {
                    S3Client = new AmazonS3Client(bucketRegion);
                    GetObjectRequest request = new GetObjectRequest
                    {
                        BucketName = s3Event.Bucket.Name,
                        Key = s3Event.Object.Key
                    };

                    int count = 0;
                    int recordsUpdated = 0;
                    int recordsErrored = 0;

                    using (GetObjectResponse response = await S3Client.GetObjectAsync(request))
                    {
                        StreamReader reader;
                        using (reader = new StreamReader(response.ResponseStream, Encoding.UTF8))
                        {
                            using (var csv = new CsvReader(reader))
                            {                       
                                csv.Read();
                                csv.ReadHeader();
                               
                                while (csv.Read())
                                {
                                    if (count < 2000)
                                    {
                                        //Console.WriteLine("INFO : Week : '{0}' Day : '{1}' UPRN : '{2}'", csv.GetField<String>("Week"), csv.GetField<String>("Day"), csv.GetField<String>("UPRN"));
                                        String round = "";
                                        switch(csv.GetField<String>("Week"))
                                        {
                                            case "A":
                                                switch (csv.GetField<String>("Day"))
                                                {
                                                    case "Monday":
                                                        round = "1";
                                                        break;
                                                    case "Tuesday":
                                                        round = "2";
                                                        break;
                                                    case "Wednesday":
                                                        round = "3";
                                                        break;
                                                    case "Thursday":
                                                        round = "4";
                                                        break;
                                                    case "Friday":
                                                        round = "5";
                                                        break;
                                                    default:
                                                        Console.WriteLine("Unexpected Day found for UPRN : '{0}'", csv.GetField<String>("UPRN"));
                                                        break;
                                                }
                                                break;
                                            case "B":
                                                switch (csv.GetField<String>("Day"))
                                                {
                                                    case "Monday":
                                                        round = "7";
                                                        break;
                                                    case "Tuesday":
                                                        round = "8";
                                                        break;
                                                    case "Wednesday":
                                                        round = "9";
                                                        break;
                                                    case "Thursday":
                                                        round = "10";
                                                        break;
                                                    case "Friday":
                                                        round = "11";
                                                        break;
                                                    default:
                                                        Console.WriteLine("Unexpected Day found for UPRN : '{0}'", csv.GetField<String>("UPRN"));
                                                        break;
                                                }
                                                break;
                                            default:
                                                Console.WriteLine("Unexpected Week found for UPRN : '{0}'", csv.GetField<String>("UPRN"));
                                                break;
                                        }
                                        AmazonDynamoDBClient client = new AmazonDynamoDBClient();
                                        string tableName = "bin-details";
                                        var dynamoRequest = new UpdateItemRequest
                                        {
                                            TableName = tableName,
                                            Key = new Dictionary<string, AttributeValue>() { { "UPRN", new AttributeValue { N = csv.GetField<String>("UPRN") } } },
                                            ExpressionAttributeNames = new Dictionary<string, string>()
                                            {
                                                {"#D", "Day"},
                                                {"#R", "Round"},
                                                {"#B", "Bins2Sacks"}
                                            },
                                            ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                                            {
                                                {":day",new AttributeValue { S = csv.GetField<String>("Day")}},
                                                {":round",new AttributeValue {N = round}},
                                                {":bins2sacks",new AttributeValue {S = "Y"}}
                                            },
                                            UpdateExpression = "SET #D = :day, #R = :round, #B = :bins2sacks"
                                        };
                                        UpdateItemResponse dynamoResponse = await client.UpdateItemAsync(dynamoRequest);
                                        if (dynamoResponse.HttpStatusCode == HttpStatusCode.OK)
                                        {
                                            recordsUpdated++;
                                        }
                                        else
                                        {
                                            Console.WriteLine("DynamoResponse NOT OK : '{0}'", dynamoResponse.HttpStatusCode);
                                            recordsErrored++;                                      
                                        }
                                        
                                    }
                                    count++;
                                }
                            }
                        }
                    }
                    Console.WriteLine("Records processed   : '{0}'", count);
                    Console.WriteLine("Records updated     : '{0}'", recordsUpdated);
                    Console.WriteLine("Records not updated : '{0}'", recordsErrored);
                    return "{\"Message\":\"Found file\",\"lambdaResult\":\"Success\"}";
                }
                catch (AmazonS3Exception error)
                {
                    Console.WriteLine("ERROR : Reading Email : '{0}' when reading file", error.Message);
                    return null;
                }
                catch (Exception error)
                {
                    Console.WriteLine("ERROR : An Unknown encountered : {0}' when reading file", error.Message);
                    return null;
                }
            }
        }
    }
}
