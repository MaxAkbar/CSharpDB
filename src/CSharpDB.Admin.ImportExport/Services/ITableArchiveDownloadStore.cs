using CSharpDB.Admin.ImportExport.Models;

namespace CSharpDB.Admin.ImportExport.Services;

public interface ITableArchiveDownloadStore
{
    TableArchiveDownload Add(string path, string fileName);
    bool TryTake(string token, out TableArchiveDownload download);
}
