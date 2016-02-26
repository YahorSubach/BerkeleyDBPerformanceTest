using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using BerkeleyDB;

namespace BdbPerformTest
{
    class BdbStorage
    {
        private readonly string _dbPAth;
        private BTreeDatabase _btreeDb;
        private BTreeDatabaseConfig _btreeConfig;
        private DatabaseEnvironment _env;
        private string _name;

        public BdbStorage(string path, string name = null, DatabaseEnvironment env = null)
        {
            _dbPAth = path;
            _env = env;
            _name = name;
            OpenBase();
        }

        private void OpenBase()
        {
            var pathStr = Environment.GetEnvironmentVariable("PATH");

            var pwd = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            pwd = Path.Combine(pwd, IntPtr.Size == 4 ? "x86" : "x64");
            if (pathStr != null && !pathStr.Contains(pwd))
            {
                pwd += ";" + Environment.GetEnvironmentVariable("PATH");
                Environment.SetEnvironmentVariable("PATH", pwd);
            }


            _btreeConfig = new BTreeDatabaseConfig
            {
                Duplicates = DuplicatesPolicy.NONE,
                ErrorPrefix = "QH_" + Path.GetFileName(_dbPAth),
                Creation = CreatePolicy.IF_NEEDED,
                FreeThreaded = true
            };

            if (_env != null)
            {
                _btreeConfig.Env = _env;
            }

            _btreeDb = BTreeDatabase.Open(_dbPAth, _name, _btreeConfig);
        }

        public void Set(DatabaseEntry key, DatabaseEntry value, bool flush = false)
        {
            _btreeDb.Put(key, value);

            if (flush)
                _btreeDb.Sync();
        }
        public void SetAll(IEnumerable<KeyValuePair<DatabaseEntry, DatabaseEntry>> initialData, bool flush = true)
        {
            _btreeDb.Truncate();

            foreach (var kv in initialData)
                Set(kv.Key, kv.Value, false);

            if (flush)
                _btreeDb.Sync();
        }


        public KeyValuePair<DatabaseEntry, DatabaseEntry> Get(DatabaseEntry key)
        {
            
            var ret = _btreeDb.Get(key);
            return ret;
        }

        public IEnumerable<KeyValuePair<DatabaseEntry, DatabaseEntry>> GetAll()
        {
            Cursor dbc;

            using (dbc = _btreeDb.Cursor())
            {
                dbc.MoveFirst();

                var dbEntry = dbc.Current;
                while (dbEntry.Key != null && dbEntry.Value != null)
                {
                    yield return new KeyValuePair<DatabaseEntry, DatabaseEntry>((dbEntry.Key),
                                                              (dbEntry.Value));
                    dbc.MoveNext();
                    dbEntry = dbc.Current;
                }
            }
        }

        public void Close()
        {
            _btreeDb.Close();
        }
    }
}
