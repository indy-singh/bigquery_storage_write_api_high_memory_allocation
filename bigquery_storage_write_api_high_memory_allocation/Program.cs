using System.Diagnostics;

namespace bigquery_storage_write_api_high_memory_allocation;

public class Program
{
    public static void Main(string[] args)
    {
        AppDomain.MonitoringIsEnabled = true;
        // fix the random so we have a repeatability of data.

        new LegacyBigQuerySaver().LegacyInsert(DummyData.Get());

        Console.WriteLine($"Took: {AppDomain.CurrentDomain.MonitoringTotalProcessorTime.TotalMilliseconds:#,###} ms");
        Console.WriteLine($"Allocated: {AppDomain.CurrentDomain.MonitoringTotalAllocatedMemorySize / 1024:#,#} kb");
        Console.WriteLine($"Peak Working Set: {Process.GetCurrentProcess().PeakWorkingSet64 / 1024:#,#} kb");

        for (var index = 0; index <= GC.MaxGeneration; index++)
            Console.WriteLine($"Gen {index} collections: {GC.CollectionCount(index)}");

    }
}