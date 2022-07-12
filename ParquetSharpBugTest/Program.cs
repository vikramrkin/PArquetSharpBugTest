using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ParquetSharp;

namespace ParquetSharpBugTest
{
    internal class Program
    {
        static void Main(string[] args)
        {
            const string filePath = ".\\Input_which_fails.parquet";
            
            using (var parquetReader = new ParquetFileReader(filePath))
            {
                var fileMetaData = parquetReader.FileMetaData;

                var numRowGroups = fileMetaData.NumRowGroups;
                var numColumns = fileMetaData.NumColumns;

                for (var g = 0; g < numRowGroups; g++)
                {
                    using (var rowGroupReader = parquetReader.RowGroup(g))
                    {
                        var rowGroupMetaData = rowGroupReader.MetaData;
                        var numRows = checked((int)rowGroupMetaData.NumRows);


                        var c0 = rowGroupReader.Column(0).LogicalReader<string>().ReadAll(numRows);
                        var c1 = rowGroupReader.Column(1).LogicalReader<string>().ReadAll(numRows);
                        var c2 = rowGroupReader.Column(2).LogicalReader<string>().ReadAll(numRows);
                        var c3 = rowGroupReader.Column(3).LogicalReader<string>().ReadAll(numRows);
                        var c4 = rowGroupReader.Column(4).LogicalReader<string>().ReadAll(numRows);
                        var c5 = rowGroupReader.Column(5).LogicalReader<string>().ReadAll(numRows);
                        var c6 = rowGroupReader.Column(6).LogicalReader<string>().ReadAll(numRows);
                        var c7 = rowGroupReader.Column(7).LogicalReader<string>().ReadAll(numRows);

                        try
                        {
                            var c8 = rowGroupReader.Column(8).LogicalReader<string>().ReadAll(numRows); //THIS CALL FAILS
                        }
                        catch(Exception ex)
                        {
                            Console.WriteLine("Exception: {0}", ex);
                        }
                        
                        var c9 = rowGroupReader.Column(9).LogicalReader<string>().ReadAll(numRows);
                    }
                }
            }
        }
    }
}
