﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLiteDump.Job.SQLiteAccess
{
    public enum TriggerEvent
    {
        Delete,
        Update,
        Insert
    }

    public enum TriggerType
    {
        After,
        Before
    }

    public class TriggerSchema
    {
        public string Name;
        public TriggerEvent Event;
        public TriggerType Type;
        public string Body;
        public string Table;
    }
}
