namespace WordList.Processing.UploadSourceChunks.Models;

public class UploadSourceChunksMessage
{
    public required string SourceId { get; set; }
    public bool ReplaceExistingWords { get; set; } = false;
}