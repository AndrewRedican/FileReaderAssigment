using System;
using System.Linq;
using System.Data;
using System.Collections.Generic;
using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

// todo: Functionaliy has alredy been split into method. Pending to add test files.

namespace UserEmailPreferencesFunction
{
    public static class UserPreferenceChangeDetection
    {
        static string fileName = "user-preferences.txt";

        [FunctionName("Function1")]
        public static void Run([BlobTrigger("dev/{name}", Connection = "AzureWebJobsStorage")] Stream blob, string name, ILogger log)
        {

            if (name == fileName)
            {
                try
                {
                    Console.WriteLine($"Processing file {name} with size {blob.Length} Bytes");
                    string fileContent = GetFileContent(blob, name);
                    string[] lines = GetLines(fileContent);
                    string[] headers = GetColumnHeaders(lines);
                    string[][] records = GetRecords(lines, headers.Length);
                    DataTable table = CreateTable(headers, records);
                    DataView view = new DataView(table);
                    view.Sort = "IsUnsubscribed DESC, AllowMarketingEmails ASC"; // todo: For some reason sorting by AllowMarketingSMS ASC gives out
                    DataTable sortedTable = view.ToTable();
                    Print(sortedTable);
                }
                catch (Exception e)
                {
                    log.LogError(e.Message);
                    Console.WriteLine(e.ToString());
                }
            }
        }

        public static string GetFileContent(Stream myBlob, string name)
        {
            StreamReader streamReader = new StreamReader(myBlob);
            return streamReader.ReadToEnd();
        }

        public static string[] GetLines(string content)
        {
            return content.Split('\n');
        }

        public static string[] GetColumnHeaders(string[] lines)
        {
            string[] headers = lines[0].Split("|");
            if (headers.Length == 0)
            {
                throw new Exception("Could extract file headers");
            }
            return headers;
        }

        public static string[][] GetRecords(string[] allLines, int fieldCount)
        {
            string[] lines = allLines.Skip(1).ToArray();
            List<string[]> records = new List<string[]>();
            foreach (string line in lines)
            {
                if (line != "")
                {
                    records.Add(line.Split("|"));
                }
            }
            return records.ToArray();
        }

        public static DataTable CreateTable(string[] headers, string[][] records)
        {
            DataTable table = new DataTable("UserEmailPreferences");
            DataColumn column;
            DataRow row;

            // Add Columns to Table
            foreach (string header in headers)
            {
                column = new DataColumn();
                column.DataType = Type.GetType("System.String"); // todo: Could be converted to type Bool
                column.ColumnName = header;
                column.ReadOnly = true;
                table.Columns.Add(column);
            }

            // Make "UserID" the primary key column
            if (!Array.Exists(headers, header => header == "UserID"))
            {
                throw new Exception("Expected 'UserID' column to exist.");
            }
            DataColumn[] PrimaryKeyColumns = new DataColumn[1];
            PrimaryKeyColumns[0] = table.Columns["UserID"];
            PrimaryKeyColumns[0].Unique = true;
            table.PrimaryKey = PrimaryKeyColumns;

            // Add Rows to Table
            foreach (string[] record in records)
            {
                row = table.NewRow();
                int colIndex = 0;
                foreach (string header in headers)
                {
                    row[header] = record[colIndex];
                    colIndex++;
                }
                table.Rows.Add(row);
            }
            return table;
        }

        public static void Print(DataTable table)
        {
            int count = 0;
            string[] columnNames = new string[table.Columns.Count];
            foreach(DataColumn column in table.Columns)
            {
                columnNames[count] = table.Columns[count].ColumnName;
                count++;
            }
            Console.WriteLine(String.Join(" | ", columnNames));

            foreach(DataRow row in table.Rows)
            {
                Console.WriteLine(String.Join(" | ", row.ItemArray));
            }
        }
    }
}
