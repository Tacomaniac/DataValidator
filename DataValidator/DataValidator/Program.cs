using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace DataValidator
{ 
    enum ErrorLevel
    {
        Error,
        Warning
    }

    interface IValidationRule<T>
    {
        string Validate(T input);
    }

    class Program
    {
        static void Main(string[] args)
        {

            string connString = @"Data Source=NARC;Initial Catalog=ETLDB;Integrated Security=True";
            string sql = @"
                             SELECT 
			                             id
			                            ,first_name = CASE WHEN RowNumber = 1 THEN first_name + CONVERT(VARCHAR,1) ELSE first_name END
			                            ,last_name
			                            ,email
			                            ,gender
			                            ,ip_address
                            FROM (
		                            SELECT 
			                             id
			                            ,first_name 
			                            ,last_name
			                            ,email
			                            ,gender
			                            ,ip_address
			                            ,RowNumber = ROW_NUMBER() OVER(ORDER BY (SELECT 1))%100
		                            FROM TestMockData
	                            ) t
                        ";
            Console.WriteLine("Begin Query...............................................");
            List<Record> recList = new List<Record>();
            //List<KeyValuePair<string, string>> failures = new List<KeyValuePair<string, string>>();


            using (SqlConnection conn = new SqlConnection(connString))
            {
                conn.Open();
                SqlCommand cmd = new SqlCommand(sql, conn);
                SqlDataReader dr = cmd.ExecuteReader();

                int i = 0;
                int batchnum = 1;

                while (dr.Read())
                {   
                        Record rec = new Record();
                        rec.id = dr[0].ToString();
                        rec.firstname = dr[1].ToString();
                        rec.lastname = dr[2].ToString();
                        rec.email = dr[3].ToString();
                        rec.gender = dr[4].ToString();
                        rec.ip_address = dr[5].ToString();
                        recList.Add(rec);
                        i++;  


                    if(i >= 5000)
                    {
                        i = 0;
                        batchnum++;
                        Console.WriteLine("BatchNumber: " + batchnum.ToString());
                        writeErrorsToSql(recList,connString);
                        recList.Clear();
                    }
                }  
            } 

            //List<KeyValuePair<string,string>> failures = new List<KeyValuePair<string, string>>();
            //failures = ValidateRecords(recList);


            //if (failures.Count() == 0)
            //{
            //    Console.WriteLine("No Records errored");
            //}
            //else
            //{
            //    foreach (KeyValuePair<string,string> failure in failures)
            //    {
            //        Console.WriteLine("Failure on " + failure.Key.ToString() + ": " + failure.Value.ToString());
            //    }
            //}

            Console.WriteLine("End Query...............................................");
            Console.ReadKey();


        }

        static void writeErrorsToSql(List<Record> records, string connString)
        {
                List<KeyValuePair<string, string>> failures = new List<KeyValuePair<string, string>>();

                failures = ValidateRecords(records);
              
                using (SqlConnection writeConn = new SqlConnection(connString))
                {
                    writeConn.Open();
                    SqlCommand sqlWriteCMD =
                    new SqlCommand(
                        @"INSERT INTO TestMockDataError ( 
	                                         [id] 
	                                        ,[ErrorReason] 
                                  ) 
                                  VALUES (
	                                         @id 
	                                        ,@ErrorReason 
                                  )");

                    sqlWriteCMD.CommandType = CommandType.Text;
                    sqlWriteCMD.Connection = writeConn;
                    sqlWriteCMD.Parameters.AddWithValue("@id", DbType.String);
                    sqlWriteCMD.Parameters.AddWithValue("@ErrorReason", DbType.String);


                    foreach (KeyValuePair<string, string> item in failures)
                    {
                        //Console.WriteLine("Failure on " + item.Key.ToString() + ": " + item.Value.ToString());
                        sqlWriteCMD.Parameters["@id"].Value = item.Key.ToString();
                        sqlWriteCMD.Parameters["@ErrorReason"].Value = item.Value.ToString();
                        sqlWriteCMD.ExecuteNonQuery();
                    }
                    writeConn.Close();
                }  
            }


        static List<KeyValuePair<string, string>> ValidateRecords(List<Record> recs)
        {
            
            List<KeyValuePair<string, string>> failures = new List<KeyValuePair<string, string>>();

            foreach (Record rec in recs)
            {
                PropertyInfo[] properties = typeof(Record).GetProperties();

                foreach (PropertyInfo property in properties)
                {
                    //Get all attribute based ValidationRules
                    var rules = Array.ConvertAll(property.GetCustomAttributes(typeof(IValidationRule<Record>), true), item => (IValidationRule<Record>)item);
                    //Gather failures
                    var tempfailures = rules.Select(r => r.Validate(rec)).Where(f => f != null);


                    foreach(string val in tempfailures)
                    {
                        failures.Add(new KeyValuePair<string, string>(rec.id.ToString(), val));

                        //failures.Add(rec.id.ToString(),val);
                    } 

                    //if (failures.Count() > 0) isValid = false;
                }
            }

            return failures;
        } 

    } 

    //Object instance of a record
    class Record
    { 
        public string id { get; set; }

        [NoNumbers(ErrorLevel.Warning)]
        public string firstname { get; set; }
         
        public string lastname { get; set; }

        public string email { get; set; }

        public string gender { get; set; }

        public string ip_address { get; set; }

    }


    class NoNumbersAttribute : Attribute, IValidationRule<Record>
    {
        private ErrorLevel errorLevel;

        public NoNumbersAttribute(ErrorLevel errorLevel)
        {
            this.errorLevel = errorLevel;
        }

        public string Validate(Record rec)
        {
            if (rec.firstname.Any(c => char.IsDigit(c)))
            {
                return $"{errorLevel.ToString()}: 'Name' contains numeric characters. ";
            }

            return null;
        }
    }


}
