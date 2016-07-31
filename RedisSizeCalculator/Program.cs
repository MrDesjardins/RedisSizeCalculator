using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using ServiceStack.Redis;
using Newtonsoft.Json;

namespace RedisSizeCalculator
{
    class Program
    {
        private const int BufferSize = 65536;  // 64 Kilobytes
        private static string RedisHost = "127.0.0.1"; //Provide your Redis Ip Here
        private static string RedisPort = "6379"; //Provide your Redis port Here
        private static string FilePathToStoreResult = Environment.CurrentDirectory + @"\stats.csv"; 

        static void Main(string[] args)
        {
            File.Delete(FilePathToStoreResult);
            WriteLenghtForEveryKeys();
        }
        static double ConvertBytesToMegabytes(long bytes)
        {
            return (bytes / 1024f) / 1024f;
        }
        static double ConvertBytesToKilobytes(long bytes)
        {
            return (bytes / 1024f);
        }

        static void WriteLenghtForEveryKeys()
        {          
            using (var redisClient = new RedisClient(RedisHost, Convert.ToInt16(RedisPort)))
            {               
                var keys = redisClient.GetAllKeys();
                using (System.IO.StreamWriter file = new System.IO.StreamWriter(@FilePathToStoreResult, true, Encoding.UTF8, BufferSize))
                {
                    foreach (string key in keys)
                    {
                        long bytes;
                        try
                        {
                            byte[] bytarr = redisClient.Get(key);
                            bytes = bytarr.Length;
                        }
                        catch (Exception)
                        {
                            try
                            {
                                byte[][] bythsharr = redisClient.HGetAll(key);
                                bytes = bythsharr.Length;
                            }
                            catch (Exception)
                            {
                                bytes = 0;
                            }
                        }
                        double kblen = ConvertBytesToKilobytes(bytes);
                        double mblen = ConvertBytesToMegabytes(bytes);
                        var lineToOutput = $"{key},{mblen},{kblen}";
                        file.WriteLine(lineToOutput);
                    }
                }                
            }
        }
    }
}
