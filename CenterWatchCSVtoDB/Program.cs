/* Console program to transform CSV data extracted from CenterWatch.com by Ruby Watir program centerwatchdownloader.rb into format used by CenterWatch iOS and Android mobile app
 * Input - CSV file
 * Output - Table of CenterWatch studies with Geocoded data
 * Uses Geocoding.NET to geocode through Google and Bing Maps
 * Uses HtmlAgilityPack to parse HTML
 * Uses Visual Basic.NET CSV Parser
 * Uses SqlBulkCopy to move records from CSV to SQL Server
 */

using System;
using System.Data;
using Microsoft.VisualBasic.FileIO;
using System.Data.SqlClient;
using HtmlAgilityPack;
using Geocoding;
using System.Collections.Generic;
using Geocoding.Google;
using System.Linq;
using Geocoding.Microsoft;

namespace CenterWatchCSVtoDB
{
    static class Program
    {
        //SQL Server destination table - must not exist already
        static string targetTable = "dbo.CWStudies2";
        //CSV file in format created by Ruby CenterWatch downloader
        static string sourceCSV = @"C:\temp\studies5.csv";
        //Google MAPs API Key - Google is limited to 2500 free geocodes per day
        static string API_KEY = "AIzaSyBewhlperSvvuN_OUCV8s4QoIAtatxeblQ";
        //BING MAPs API key
        static string BING_KEY = "Ap-tdR7Kt1PZ1OKqG4jhhlQjSv6lepWjiEJ-Ua44_BfK3KARQHyfPPJqOO5zxpyf";
        //TODO Move to config file
        static string CONNECTION_STRING = "Data Source=(local); " + " Integrated Security=true;" + "Initial Catalog=CenterWatch;";
        //static string CONNECTION_STRING = @"Integrated Security=true; Initial Catalog=prod_centerwatch_01; Data Source = PRODSQL02;";
        static void Main()
        {
            //Transform CSV file to in-memory data table 
            string csv_file_path = sourceCSV;
            DataTable csvData = GetDataTableFromCSVFile(csv_file_path);
            Console.WriteLine("Rows count:" + csvData.Rows.Count);
            //Create SQL command for building SQL Server table from in-memory data table 
            string connectionString = GetConnectionString();
            string createTable = CreateTABLE(targetTable, csvData);
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                //Create physical SQL Server table
                connection.Open();
                SqlCommand c = new SqlCommand(createTable, connection);
                c.ExecuteNonQuery();
                //Copy all data from in-memory table to physical table - super-fast
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
                {
                bulkCopy.DestinationTableName = targetTable;
                    try
                    {
                        // Write from the source to the destination. Fast - 1000+ records per second.
                        bulkCopy.WriteToServer(csvData);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
                //Add physical columns to SQL Server table to support GeoCoding
                AddStateandCountryColumnsToTable(connection);
                //Fill state and country info in SQL table
                ExtractStateCountryFromLocation(connection);
                //FIll longitude and latitude data in SQL table
                GeocodeStudyData(connection);
            }
            Console.WriteLine("Press Enter to finish.");
            Console.ReadLine();
        }
        private static void GeocodeStudyData(SqlConnection connection)
        {   //Fill in memory table from SQL table
            string query = string.Format("SELECT * FROM {0}", targetTable);
            DataTable studies = new DataTable();
            SqlCommand cmd = new SqlCommand(query, connection);
            SqlDataAdapter adapter = new SqlDataAdapter(cmd);
            adapter.Fill(studies);
            //GeoCode SQL Data
            GeocodeLocations(studies, connection, adapter);
        }

        private static void ExecuteSQLCommand(string sqlText, SqlConnection connection)
        {
            SqlCommand sql = new SqlCommand(sqlText, connection);
            try
            {
                sql.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
        private static void AddStateandCountryColumnsToTable(SqlConnection connection)
        {
            //SQL DDL to add geographic fields and primary key to our SQL Server table
            string sqlText = string.Format(@"ALTER TABLE {0} ADD [STATE] nvarchar(2) NULL, [COUNTRY] nvarchar(100) NULL, 
                    [Latitude] FLOAT NULL, [Longitude] FLOAT NULL;", targetTable);

            ExecuteSQLCommand(sqlText, connection);

            //Add identity column
            string sqlTextI = string.Format(@"ALTER TABLE {0} ADD ID Int IDENTITY(1, 1)", targetTable);
            ExecuteSQLCommand(sqlTextI, connection);

            //Add Primary Key
            string sqlTextPK = string.Format(@"ALTER TABLE {0} ADD PRIMARY KEY (ID)", targetTable);
            ExecuteSQLCommand(sqlTextPK, connection);


            //Add Point
            string sqlTextPoint = string.Format(@"ALTER TABLE {0} ADD POINT (geography)", targetTable);
            ExecuteSQLCommand(sqlTextPoint, connection);

            //Add spatial index
            string sqlIndexPoint = string.Format(@"CREATE SPATIAL INDEX[SPATIAL_{0}] ON[dbo].[{0}]
                (
                   [Point]
                )   USING GEOGRAPHY_GRID WITH(GRIDS = (LEVEL_1 = MEDIUM, LEVEL_2 = MEDIUM, LEVEL_3 = MEDIUM, LEVEL_4 = MEDIUM), 
                    CELLS_PER_OBJECT = 16, PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, 
                    ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON[PRIMARY]", targetTable);
            ExecuteSQLCommand(sqlIndexPoint, connection);

            //Nearest Neighbors algorithm - 
            //4326 is default Geography encoding
            //Default Longitude and Latitude points are for ePS headquarters 
            //Default range is 100 miles
            string procNearestNeighbors = @"CREATE PROCEDURE[dbo].[NearestNeighbors]
                    @condition varchar(100) = '', 
	                @Latitude FLOAT = 41.27,
                    @Longitude FLOAT = -75.85,
	                @Range FLOAT = 1600000
                AS
                BEGIN
                    SET NOCOUNT ON;
	                declare @x geography = Geography::Point(@Latitude, @longitude, 4326);
	                select top(30)  [location], summary, purpose, overview, eligibility, moreinfo, contact, latitude, longitude, round((Point.STDistance(@x)/1000)  * 0.621371, 0) as Miles, CWID FROM CWSTudies
                where Point.STDistance(@x) is not null and longitude is not null and latitude is not null 
	                order by miles
                END";
            ExecuteSQLCommand(procNearestNeighbors, connection);


            string procNearestNeighborsCondition = @"CREATE PROCEDURE[dbo].[NearestNeighborsCondition]
                    @condition varchar(100) = '', 
	                @Latitude FLOAT = 41.27,
                    @Longitude FLOAT = -75.85,
	                @Range FLOAT = 1600000
                AS
                BEGIN
                    SET NOCOUNT ON;
	                declare @x geography = Geography::Point(@Latitude, @longitude, 4326);
	                select top(30)  [location], summary, purpose, overview, eligibility, moreinfo, contact, latitude, longitude, round((Point.STDistance(@x)/1000)  * 0.621371, 0) as Miles, CWID FROM CWSTudies
                where Point.STDistance(@x) is not null and condition = @condition and longitude is not null and latitude is not null 
	                order by miles
                END";
            ExecuteSQLCommand(procNearestNeighborsCondition, connection);

            string procNearestNeighborsState = @"CREATE PROCEDURE[dbo].[NearestNeighborsState]
                    @state varchar(100) = '', 
	                @Latitude FLOAT = 41.27,
                    @Longitude FLOAT = -75.85,
	                @Range FLOAT = 1600000
                AS
                BEGIN
                    SET NOCOUNT ON;
	                declare @x geography = Geography::Point(@Latitude, @longitude, 4326);
	                select top(30)  [location], summary, purpose, overview, eligibility, moreinfo, contact, latitude, longitude, round((Point.STDistance(@x)/1000)  * 0.621371, 0) as Miles, CWID FROM CWSTudies
                where Point.STDistance(@x) is not null and [state] = @state and longitude is not null and latitude is not null 
	                order by miles
                END";
            ExecuteSQLCommand(procNearestNeighborsState, connection);

            string procNearestNeighborsCountry = @"CREATE PROCEDURE[dbo].[NearestNeighborsCountry]
                    @country varchar(100) = '', 
	                @Latitude FLOAT = 41.27,
                    @Longitude FLOAT = -75.85,
	                @Range FLOAT = 1600000
                AS
                BEGIN
                    SET NOCOUNT ON;
	                declare @x geography = Geography::Point(@Latitude, @longitude, 4326);
	                select top(30)  [location], summary, purpose, overview, eligibility, moreinfo, contact, latitude, longitude, round((Point.STDistance(@x)/1000)  * 0.621371, 0) as Miles, CWID FROM CWSTudies
                where Point.STDistance(@x) is not null and [country] = @country and longitude is not null and latitude is not null 
	                order by miles
                END";
            ExecuteSQLCommand(procNearestNeighborsCountry, connection);

            string geoCodePoints = string.Format(@"update {0} set[point] = geography::Point(Latitude, Longitude, 4326) where Longitude is not null and latitude is not null", targetTable);
            ExecuteSQLCommand(geoCodePoints, connection);
        }

        private static void GeocodeLocations(DataTable dtData, SqlConnection connection, SqlDataAdapter adapter)
        {   //iterate through Datable and GeoCode contact field
            //The contact field contains HTML contact information extracted from CenterWatch including a Google Maps reference
            SqlCommandBuilder cb = new SqlCommandBuilder(adapter);
            foreach (DataRow row in dtData.Rows)
            {
                Address address = GeocodeLocation(row["Contact"].ToString());
                if (!(address == null))
                {
                    row["Latitude"] = address.Coordinates.Latitude;
                    row["Longitude"] = address.Coordinates.Longitude;
                }
                else Console.WriteLine("Error:" + row["CWID"].ToString());
            }
            //Persist im-memory updates to database
            adapter.UpdateCommand = cb.GetUpdateCommand();
            adapter.Update(dtData);
        }
        private static Address GeocodeLocation(string location)
        {
            HtmlDocument site = new HtmlDocument();
            site.LoadHtml(location);
            //Find all links in contact HTML
            HtmlNodeCollection links = site.DocumentNode.SelectNodes("//a[@href]");
            if (!(links == null))
            {
                foreach (HtmlNode link in links)
                {
                    HtmlAttribute gMapLink = link.Attributes["href"];
                    if (gMapLink.Value.IndexOf("maps.google") > 0)
                    {
                        //if Link is a google maps link, geocode it
                        return Geocode(gMapLink);
                    }
                }
            }
            return null;
        }
         private static Address Geocode(HtmlAttribute gMapLink)
        {
            string address = gMapLink.Value;
            //remove '.', ',', ':' and other characters that cause Geocoding to throw exception  
            address = CleanAddress(address);
                
            //Commented out Google Maps API because of 2500 request limit.
                //IGeocoder geocoder = new GoogleGeocoder() { ApiKey = API_KEY };
             //Use BING API 
            IGeocoder geocoder = new BingMapsGeocoder(BING_KEY);
            
            if (address.IndexOf("%") < 0) //Check for %20a and other problem encoding
            {
                //Call to Bing's rest spatial data service 
                IEnumerable<Address> addresses = geocoder.Geocode(address);
                try
                {
                    if (addresses.Count() > 1) //geocoding was successful
                    {
                        Console.WriteLine("Formatted: " + addresses.First().FormattedAddress); //Formatted: 1600 Pennslyvania Avenue Northwest, Presiden'ts Park, Washington, DC 20500, USA
                        Console.WriteLine("Coordinates: " + addresses.First().Coordinates.Latitude + ", " + addresses.First().Coordinates.Longitude);
                        return addresses.First();
                    }
                    else return null; //geocoding did not return an address
                }
                catch (Exception ex)
                {
                    //Geocoding threw Exception
                    Console.WriteLine("Geocoding error: " + ex.InnerException + " " + ex.Message);
                    return null;
                }
            }
            else
            {
                //GMaps HTML string was badly formatted on CenterWatch
                Console.WriteLine("Bad Address " + address);
                return null;
            }
        }

        private static string CleanAddress(string address)
        {
            address = address.Replace('+', ' ');
            address = address.Replace('.', ' ');
            address = address.Replace(',', ' ');
            address = address.Replace(':', ' ');
            address = address.Replace(@"Located In%3a", " ");
            address = address.Replace(@"Located in%3a", " ");
            address = address.Replace(@"%2c", " ");
            address = address.Replace(@"%0d%0a", " ");
            address = address.Replace(@"%3c%", " ");
            address = address.Replace("http://maps.google.com/maps?q=", "");
            return address;
        }

        private static void ExtractStateCountryFromLocation(SqlConnection connection)
        {
            //Update state and country in SQL Server table by extracting from CenterWatch location data
            string sqlText = string.Format(@"update {0}
              set[State] = case 
                            WHEN substring(location, 14, 1) = ',' THEN
                              substring(location, 16, 2)
                            ELSE ''
                          end,
                [Country] = Case WHEN substring(location, 14, 1) = ',' THEN
                                substring(location, 1, 13)
				            else location
                            end", targetTable);
            SqlCommand sql = new SqlCommand(sqlText, connection);
            try
            {
                sql.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
        private static DataTable GetDataTableFromCSVFile(string csv_file_path)
        {
            //Read CSV Data into in-memory table. This is fast using the VB.NET TextFieldParser
            DataTable csvData = new DataTable();
            try
            {
                using (TextFieldParser csvReader = new TextFieldParser(csv_file_path))
                {
                    csvReader.SetDelimiters(new string[] { "," });
                    csvReader.HasFieldsEnclosedInQuotes = true;
                    //read column names
                    string[] colFields = csvReader.ReadFields();
                    foreach (string column in colFields)
                    {
                        DataColumn datecolumn = new DataColumn(column);
                        datecolumn.AllowDBNull = true;
                        csvData.Columns.Add(datecolumn);
                    }
                    while (!csvReader.EndOfData)
                    {
                        string[] fieldData = csvReader.ReadFields();
                        //Making empty value as null
                        for (int i = 0; i < fieldData.Length; i++)
                        {
                            if (fieldData[i] == "")
                            {
                                fieldData[i] = null;
                            }
                        }
                        csvData.Rows.Add(fieldData);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            return csvData;
        }
        private static string GetConnectionString()
        // To avoid storing the connection string in your code, 
        // you can retrieve it from a configuration file. 
        {
            return CONNECTION_STRING;
        }
        public static string CreateTABLE(string tableName, DataTable table)
        {   //Create a DDL SQL Create Table statement from an in-memory table
            string sqlsc;
            sqlsc = "CREATE TABLE " + tableName + "(";
            for (int i = 0; i < table.Columns.Count; i++)
            {
                sqlsc += "\n [" + table.Columns[i].ColumnName + "] ";
                string columnType = table.Columns[i].DataType.ToString();
                switch (columnType)
                {
                    case "System.Int32":
                        sqlsc += " int ";
                        break;
                    case "System.Int64":
                        sqlsc += " bigint ";
                        break;
                    case "System.Int16":
                        sqlsc += " smallint";
                        break;
                    case "System.Byte":
                        sqlsc += " tinyint";
                        break;
                    case "System.Decimal":
                        sqlsc += " decimal ";
                        break;
                    case "System.DateTime":
                        sqlsc += " datetime ";
                        break;
                    case "System.String":
                    default:
                        sqlsc += string.Format(" nvarchar({0}) ", table.Columns[i].MaxLength == -1 ? "max" : table.Columns[i].MaxLength.ToString());
                        break;
                }
                if (table.Columns[i].AutoIncrement)
                    sqlsc += " IDENTITY(" + table.Columns[i].AutoIncrementSeed.ToString() + "," + table.Columns[i].AutoIncrementStep.ToString() + ") ";
                if (!table.Columns[i].AllowDBNull)
                    sqlsc += " NOT NULL ";
                sqlsc += ",";
            }
            return sqlsc.Substring(0, sqlsc.Length - 1) + "\n)";
        }
    }
}