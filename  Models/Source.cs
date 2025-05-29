using Amazon.DynamoDBv2.DataModel;

namespace WordList.Processing.UploadSourceChunks.Models;

[DynamoDBTable("sources-table")]
public class Source
{
    [DynamoDBHashKey("id")]
    public required string Id { get; set; }

    [DynamoDBProperty("name")]
    public required string Name { get; set; }

    [DynamoDBProperty("url")]
    public string? Url { get; set; }
}