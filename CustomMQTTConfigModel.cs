using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using DataAcquisitionLibrary;

namespace DC_0003
{
    public class CustomMQTTConfigModel:MqttConfigModel
    {
        public string CommandTopic { get; set; }

        public string CommandResponseTopic { get; set; }


    public bool HeartbeatEnable { get; set; }
    }
}
