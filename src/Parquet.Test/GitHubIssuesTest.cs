using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Parquet.Data;
using Parquet.Data.Rows;
using Xunit;

namespace Parquet.Test
{
   /// <summary>
   /// This is intended to test specific github issues. They should be eventually merged into an appropriate test section and moved out from here.
   /// </summary>
   public class GitHubIssuesTest : TestBase
   {
      [Fact]
      public void Issue_16_437()
      {
         var idColumn = new DataColumn(
            new DataField<Int16>("id"),
            new Int16[] { 1, 2 });

         var cityColumn = new DataColumn(
            new DataField<string>("city"),
            new string[] { "London", "Derby" });

         // create file schema
         var schema = new Schema(idColumn.Field, cityColumn.Field);

         using (Stream fileStream = System.IO.File.OpenWrite("c:\\tmp\\test.parquet"))
         {
            using (var parquetWriter = new ParquetWriter(schema, fileStream))
            {
               // create a new row group in the file
               using (ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup())
               {
                  groupWriter.WriteColumn(idColumn);
                  groupWriter.WriteColumn(cityColumn);
               }
            }
         }

         string fileNameX = @"c:\tmp\test.parquet";
         Stream fs = new FileStream(fileNameX, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
         Table z = ParquetReader.ReadTableFromStream(fs);
      }
   }
}
