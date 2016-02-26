using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BerkeleyDB;
using System.Reflection;

namespace BdbPerformTest
{
    class BdbPerformTest
    {
        static public DatabaseEntry GetNextDBEntry(DatabaseEntry key)
        {

            long FirstPart;
            if (key.Data != null)
            {
                using (var ms = new MemoryStream(key.Data))
                {
                    using (var bw = new BinaryReader(ms))
                    {
                        FirstPart = bw.ReadInt64();
                        bw.Close();
                    }
                    ms.Close();
                }
            }
            else
            {
                FirstPart = 0;
            }
            int count = sizeof(long) + sizeof(byte) + sizeof(byte);
            var data = new byte[count];

            using (var ms = new MemoryStream(data))
            {
                using (var bw = new BinaryWriter(ms))
                {
                    bw.Write((FirstPart+1));
                    bw.Write((byte)0);
                    bw.Write((byte)0);
                    bw.Close();
                }
                ms.Close();
            }
            return new DatabaseEntry(data);

        }


        static public void DoTest(int BaseNum, int RecordsNumber)
        {
            {
                Stopwatch watch = new Stopwatch();
                String BasePath = "C://BdbPerformanceTestFolder";
                System.Console.WriteLine();

                int envOpenTime = 0;
                int baseOpenTime = 0;
                int oneBaseSetTime = 0;
                int multiBaseSetTime = 0;
                int oneBaseGetTime = 0;
                int multiBaseGetTime = 0;

                int gettedM1ask;
                int gettedM1bid;
                int gettedTks;
                int gettedTksLv2;

                List<double> oneSpeeds = new List<double>();

                int gettedElements = 0;

                for (int baseFileID = 0; baseFileID < BaseNum; baseFileID++)
                {
                    gettedM1ask = 0;
                    gettedM1bid = 0;
                    gettedTks = 0;
                    gettedTksLv2 = 0;

                    var envConfig = new DatabaseEnvironmentConfig();


                    envConfig.Create = true;
                    envConfig.DataDirs.Add(BasePath + "//" + baseFileID.ToString());
                    envConfig.CreationDir = BasePath + "//" + baseFileID.ToString();
                    envConfig.ErrorPrefix = "QH_BDB_ENV_" + baseFileID.ToString();
                    envConfig.UseLocking = true;
                    envConfig.UseMPool = true;
                    envConfig.FreeThreaded = true;

                    Directory.CreateDirectory(BasePath + "//" + baseFileID.ToString());
                    DatabaseEnvironment env = null;
                    watch.Reset();
                    watch.Start();
                    env = DatabaseEnvironment.Open(BasePath + "//" + baseFileID.ToString(), envConfig);
                    watch.Stop();
                    envOpenTime += (int)watch.ElapsedMilliseconds;


                    watch.Reset();
                    watch.Start();
                    var BaseM1Ask = new BdbStorage(BasePath + "//" + baseFileID.ToString() + "//" + baseFileID.ToString() + ".db", "M1 ask", env);
                    var BaseM1Bid = new BdbStorage(BasePath + "//" + baseFileID.ToString() + "//" + baseFileID.ToString() + ".db", "M1 bid", env);
                    var BaseTks = new BdbStorage(BasePath + "//" + baseFileID.ToString() + "//" + baseFileID.ToString() + ".db", "ticks", env);
                    var BaseTksL2 = new BdbStorage(BasePath + "//" + baseFileID.ToString() + "//" + baseFileID.ToString() + ".db", "ticks level2", env);
                    watch.Stop();
                    baseOpenTime += (int)watch.ElapsedMilliseconds;

                    var listToAppend = new List<KeyValuePair<DatabaseEntry, DatabaseEntry>>();
                    var key = new DatabaseEntry();
                    var value = new DatabaseEntry();
                    for (int recNum = 0; recNum < RecordsNumber; recNum++)
                    {
                        key = GetNextDBEntry(key);
                        value= GetNextDBEntry(value);
                        listToAppend.Add(new KeyValuePair<DatabaseEntry, DatabaseEntry>(key, value));
                    }


                    watch.Reset();
                    watch.Start();
                    BaseM1Ask.SetAll(listToAppend);
                    watch.Stop();
                    if (oneBaseSetTime == 0)
                        oneBaseSetTime += (int)watch.ElapsedMilliseconds;
                    multiBaseSetTime += (int)watch.ElapsedMilliseconds;
                    watch.Reset();
                    watch.Start();
                    BaseM1Bid.SetAll(listToAppend);
                    BaseTks.SetAll(listToAppend);
                    BaseTksL2.SetAll(listToAppend);
                    watch.Stop();
                    multiBaseSetTime += (int)watch.ElapsedMilliseconds;

                    
                    var listToGet = new List<KeyValuePair<DatabaseEntry, DatabaseEntry>>();



                    watch.Reset();
                    watch.Start();
                    foreach (var kv in BaseM1Ask.GetAll())
                    {
                        gettedM1ask += 1;
                    }
                    watch.Stop();
                    gettedElements += gettedM1ask;
                    oneSpeeds.Add(1.0* gettedM1ask*1000/ watch.ElapsedMilliseconds);
                    multiBaseGetTime += (int)watch.ElapsedMilliseconds;


                    watch.Reset();
                    watch.Start();
                    foreach (var kv in BaseM1Bid.GetAll())
                    {
                        gettedM1bid += 1;
                    }
                    watch.Stop();
                    gettedElements += gettedM1bid;
                    oneSpeeds.Add(1.0 * gettedM1bid * 1000 / watch.ElapsedMilliseconds);
                    multiBaseGetTime += (int)watch.ElapsedMilliseconds;


                    watch.Reset();
                    watch.Start();
                    foreach (var kv in BaseTks.GetAll())
                    {
                        gettedTks += 1;
                    }
                    watch.Stop();
                    gettedElements += gettedTks;
                    oneSpeeds.Add(1.0 * gettedTks * 1000 / watch.ElapsedMilliseconds);
                    multiBaseGetTime += (int)watch.ElapsedMilliseconds;

                    watch.Reset();
                    watch.Start();
                    foreach (var kv in BaseTksL2.GetAll())
                    {
                        gettedTksLv2 += 1;
                    }
                    watch.Stop();
                    gettedElements += gettedTksLv2;
                    oneSpeeds.Add(1.0 * gettedTksLv2 * 1000 / watch.ElapsedMilliseconds);
                    multiBaseGetTime += (int)watch.ElapsedMilliseconds;

                    BaseM1Ask.Close();
                    BaseM1Bid.Close();
                    BaseTks.Close();
                    BaseTksL2.Close();

                    env.Close();

                    Directory.Delete(BasePath,true);


                }

                System.Console.WriteLine( "--- Bdb Performance Test with " + BaseNum.ToString() + " files with 4x" + BaseNum.ToString() + " databases and " + RecordsNumber + " records in each database ---\n");
                System.Console.WriteLine( BaseNum.ToString() + " environements open time: " + envOpenTime.ToString() + " ms");
                System.Console.WriteLine( BaseNum.ToString() + "x4" + " bases open time: " + baseOpenTime.ToString() + " ms");
                System.Console.WriteLine( RecordsNumber.ToString() + " records setting to one base: " + oneBaseSetTime.ToString() + " ms");
                System.Console.WriteLine( RecordsNumber.ToString() + "x4x" + BaseNum.ToString() + "=" + (RecordsNumber * 4 * BaseNum).ToString() + " records setting to all bases: " + multiBaseSetTime.ToString() + " ms");
                System.Console.WriteLine("\nAll bases getting speed: " + (1.0 * gettedElements * 1000 / multiBaseGetTime).ToString() + " rec/s");
                System.Console.WriteLine("\nOne base getting speed:");
                System.Console.WriteLine("min\t:\t" + oneSpeeds.Min().ToString()+" rec/s");
                System.Console.WriteLine("mean\t:\t" + oneSpeeds.Average(s => s).ToString() + " rec/s");
                System.Console.WriteLine("max\t:\t" + oneSpeeds.Max().ToString() + " rec/s");
                System.Console.WriteLine("\nGetting of all "+ gettedElements.ToString()+" records time: " + multiBaseGetTime.ToString() + " ms");
                System.Console.WriteLine("-----------------------------------------------\n\n\n");


            }
        }

        static void Main(string[] args)
        {
            var pathStr = Environment.GetEnvironmentVariable("PATH");

            var pwd = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            pwd = Path.Combine(pwd, IntPtr.Size == 4 ? "x86" : "x64");
            if (pathStr != null && !pathStr.Contains(pwd))
            {
                pwd += ";" + Environment.GetEnvironmentVariable("PATH");
                Environment.SetEnvironmentVariable("PATH", pwd);
            }
            DoTest(1, 100000);
            DoTest(1, 1000);
            DoTest(50, 1000);
            DoTest(1, 10000);
            DoTest(30, 10000);

            System.Console.WriteLine("\nEnd of tests       ...press enter");
            Console.ReadLine();
        }
    }
}
