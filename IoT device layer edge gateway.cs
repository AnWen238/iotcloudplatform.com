
/*
*Project features:IoT device layer edge gateway
*IOT cloud platform description:IOT Cloud Platform (blog.iotcloudplatform.com) focuses on IoT design, IoT programming, security IoT, industrial IoT, military IoT, best IoT projects, IoT ideas, IoT companies, Chinese IoT companies, American IoT companies, top IoT companies and other technological knowledge.
*Author: https://blog.iotcloudplatform.com/
*/
using HslCommunication;
using HslCommunication.Core;
using HslCommunication.Core.Device;
using HslCommunication.ModBus;
using IotDataFlow.Section.gate.model;
using IotDataFlow.Section.iot.model;
using IotDataFlow.Util;
using Microsoft.ClearScript.V8;
using MQTTnet.Client;
using System.Text;
using System.Text.Json;

namespace IotDataFlow.Section.gate
{
    /// <summary>
    /// 【设备层】边缘网关
    /// </summary>
    public class Gate
    {
        static GateModel GateModel_Time = new GateModel();
        static GateModel GateModel_Change = new GateModel();
        static Dictionary<string, EquipModel> DicTag = new Dictionary<string, EquipModel>();
        static MQClient MqttGate = new MQClient();
        static string ScriptPathGate = AppDomain.CurrentDomain.BaseDirectory + $"Script{Path.DirectorySeparatorChar}gate{Path.DirectorySeparatorChar}script1.js";
        static V8ScriptEngine Engine = new V8ScriptEngine();
        static string PubTopic;

        /// <summary>
        /// 配置基础信息:网关信息
        /// </summary>
        public static void ConfigBaseInfo()
        {
            try
            { 
                GateModel_Time.GateCode = "gw1";
                GateModel_Time.Equips = new List<EquipModel>()
                {
                    new EquipModel()
                    {
                        EquipCode = "JY355",
                        HostAddress = "127.0.0.1",
                        PortNumber = 502,
                        Tags = new List<TagModel>()
                        {
                            new TagModel() { TagCode = "40105", TagDT = (typeof(ushort)).Name.ToLower(), TagValue = 0 },
                            new TagModel() { TagCode = "40106", TagDT = (typeof(ushort)).Name.ToLower(), TagValue = 0 },
                            new TagModel() { TagCode = "00105", TagDT = (typeof(bool)).Name.ToLower(), TagValue = false },
                            new TagModel() { TagCode = "00106", TagDT = (typeof(bool)).Name.ToLower(), TagValue = false }
                        }
                    }
                };

                // 添加地址与数据类型对应关系，用于写入时查询
                foreach (var equip in GateModel_Time.Equips)
                {
                    foreach (var tag in equip.Tags)
                    {
                        if (!DicTag.ContainsKey(tag.TagCode))
                        {
                            DicTag.Add(tag.TagCode, equip);
                        }
                    }
                }

                // 变化上传Model
                GateModel_Change.GateCode = GateModel_Time.GateCode;
                GateModel_Change.Equips = new List<EquipModel>() { new EquipModel() { Tags = new List<TagModel>() { new TagModel() } } };
            }
            catch (Exception ex)
            {
                Logger.Error($"Gate,ConfigBaseInfo,errmsg:{ex.Message}\r\nstacktrace:{ex.StackTrace}");
            }
        }

        /// <summary>
        /// 运行服务(定时轮询&定时上报)
        /// </summary>
        public static async Task Run(MqttModel mqModel, string subTopic, string pubTopic)
        {
            try
            {
                PubTopic = pubTopic;

                var scriptContent = File.ReadAllText(ScriptPathGate);
                Engine.Execute(scriptContent);

                await MqttGate.InitConnect("127.0.0.1", mqModel.Port, mqModel.UserName, mqModel.Password, "mqgate", subTopic, SubCallBack);

                Task.Run(() => TskPoll());
                Task.Run(() => TskUpload());
            }
            catch (Exception ex)
            {
                Logger.Error($"Gate,Run,errmsg:{ex.Message}\r\nstacktrace:{ex.StackTrace}");
            }
        }

        /// <summary>
        /// 定时轮询
        /// </summary>
        static async void TskPoll()
        { 
            while (true)
            {
                try
                {
                    foreach (var equip in GateModel_Time.Equips)
                    {
                        var hsl = GetHslBase(equip);

                        foreach (var tag in equip.Tags)
                        {
                            bool status = false; object result = null; string message = null;

                            var startaddress = tag.TagCode; ConvertStandardModbusAddress2HSLAddress(ref startaddress);

                            switch (tag.TagDT)
                            {
                                case "uint16":
                                    {
                                        OperateResult<ushort> response = await hsl.ReadUInt16Async(startaddress);
                                        if (null != response) { status = response.IsSuccess; result = response.Content; message = response.ToMessageShowString(); } 
                                    }
                                    break;
                                case "boolean":
                                    {
                                        OperateResult<bool> response = await hsl.ReadBoolAsync(startaddress);
                                        if (null != response) { status = response.IsSuccess; result = response.Content; message = response.ToMessageShowString(); }
                                    }
                                    break;
                            }

                            if (!status)
                            {
                                Logger.Error($"网关读取Modbus数据异常,address:{tag.TagCode},errmsg:{message}");
                            }
                            else
                            {
                                // 变化上报
                                if (!tag.TagValue.ToString().Equals(result.ToString()))
                                {
                                    tag.TagValue = result; 
                                    //
                                    GateModel_Change.Equips[0].EquipCode = equip.EquipCode;
                                    GateModel_Change.Equips[0].Tags[0] = tag; 
                                    UploadData("变化上报", GateModel_Change);// {"ts":"2024-09-04T03:13:41.874Z","d":[{"tag":"40105","value":0}]}
                                } 
                            }

                            await Task.Delay(100);
                        } 
                    }
                }
                catch (Exception ex)
                {
                    Logger.Error($"Gate,TskPoll,errmsg={ex.Message}\r\nstacktrace:{ex.StackTrace}");
                }

                await Task.Delay(500);
            }
        }

        /// <summary>
        /// 定时上报
        /// </summary>
        static async void TskUpload()
        { 
            while (true)
            {
                await Task.Delay(60000);

                try
                {
                    await UploadData("定时上报", GateModel_Time);// {"ts":"2024-09-03T07:57:58.471Z","d":[{"tag":"40105","value":"5"},{"tag":"40106","value":"6"}]} 
                }
                catch (Exception ex)
                {
                    Logger.Error($"Gate,TskUpload,errmsg={ex.Message}\r\nstacktrace:{ex.StackTrace}");
                } 
            }
        }

        /// <summary>
        /// 上报数据
        /// </summary>
        static async Task UploadData(string note, GateModel gateModel)
        {
            string dataJson = JsonSerializer.Serialize(gateModel);

            var encodeJson = Engine.Invoke("encodeMqttPayload", dataJson);

            Logger.Info($"网关自定义编码 >> 备注=【{note}】 Topic主题=【{PubTopic}】 消息=【{encodeJson}】");

            await MqttGate.Publish(PubTopic, encodeJson.ToString());
        }

        /// <summary>
        /// 收到消息事件
        /// </summary>
        /// <param name="arg"></param>
        /// <returns></returns> 
        static async Task SubCallBack(MqttApplicationMessageReceivedEventArgs arg)
        {
            try
            { 
                string topic = arg.ApplicationMessage.Topic;
                string mqttPayload = Encoding.UTF8.GetString(arg.ApplicationMessage.PayloadSegment); 
                Logger.Info($"网关接收报文 >> 客户端ID=【{arg.ClientId}】 Topic主题=【{topic}】 消息=【{mqttPayload}】");

                // 写入 {"ts":"2024-09-04T03:41:34.747Z","w":[{"tag":"10105"}]} 
                var decodeJson = Engine.Invoke("decodeMqttPayload", mqttPayload);

                Logger.Info($"网关自定义解码 >> {decodeJson}");

                var model = JsonSerializer.Deserialize<GateModel>(decodeJson.ToString());

                foreach (var equip in model.Equips)
                {
                    foreach (var tag in equip.Tags)
                    {
                        var tagCode = tag.TagCode;
                        var address = tagCode; ConvertStandardModbusAddress2HSLAddress(ref address);
                        var content = tag.TagValue.ToString(); OperateResult? operateResult = null; 

                        if (DicTag.ContainsKey(tagCode))
                        {
                            var tempDevice = DicTag[tagCode];

                            var hsl = GetHslBase(tempDevice);

                            var tempTag = tempDevice.Tags.Where(x => x.TagCode == tagCode).First();

                            if (null != tempTag)
                            {
                                switch (tempTag.TagDT)
                                {
                                    case "uint16":
                                        {
                                            operateResult = await hsl.WriteAsync(address, Convert.ToUInt16(content));
                                        }
                                        break;
                                    case "boolean":
                                        {
                                            operateResult = await hsl.WriteAsync(address, Convert.ToBoolean(content));
                                        }
                                        break;
                                }

                                if (null != operateResult && operateResult.IsSuccess)
                                {
                                    if (operateResult.IsSuccess)
                                    {
                                        Logger.Info($"网关写入数据 >> 状态=【成功】 地址=【{tagCode}】 值=【{content}】");
                                    }
                                    else
                                    {
                                        Logger.Error($"网关写入数据 >> 状态=【失败】 地址=【{tagCode}】 值=【{content}】错误信息=【{operateResult.ToMessageShowString()}】");
                                    }
                                }
                                else
                                {
                                    Logger.Error($"网关写入数据 >> 状态=【失败】 地址=【{tagCode}】 值=【{content}】 错误信息=【operateResult==null】");
                                }
                            }
                            else
                            {
                                Logger.Error($"网关写入数据 >> 状态=【失败】 地址=【{tagCode}】 值=【{content}】 错误信息=【tempTag==null】");
                            } 
                        }
                        else
                        {
                            Logger.Error($"网关写入数据 >> 状态=【失败】 地址=【{tagCode}】 值=【{content}】错误信息=【DicTag字典查无数据】");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"Gate,SubCallBack,errmsg:{ex.Message}\r\nstacktrace:{ex.StackTrace}");
            }
        }

        /// <summary>
        /// 获取Hsl进行设备交互的对象
        /// </summary>
        static DeviceCommunication GetHslBase(EquipModel equip)
        {
            var hsl = (DeviceCommunication)UidMgr.GetClient(equip.EquipCode);
            if (null == hsl)
            {
                var temp = new ModbusTcpNet(equip.HostAddress, equip.PortNumber);
                temp.Station = 1;
                temp.AddressStartWithZero = false;
                temp.DataFormat = DataFormat.CDAB;
                temp.ReceiveTimeOut = 5000;
                hsl = temp;
                UidMgr.AddClient(equip.EquipCode, hsl);
                Logger.Info($"网关初始化设备交互 >> 设备编码=【{equip.EquipCode}】"); 
            }
            return hsl;
        }

        /// <summary>
        /// 地址转换
        /// </summary>
        /// <param name="val"></param>
        static void ConvertStandardModbusAddress2HSLAddress(ref string val)
        {
            if (!val.Contains("x="))
            {
                int code = 1;
                ushort address = Convert.ToUInt16(val);
                if (address >= 00001 && address <= 09999)// 00001 ~ 09999
                {
                    code = 1;// 读线圈状态
                }
                else if (address >= 10001 && address <= 19999)// 10001 ~ 19999
                {
                    code = 2;// 读离散输入状态（只读）
                }
                else if (address >= 30001 && address <= 39999)// 30001 ~ 39999 04指令
                {
                    code = 4;// 读输入寄存器（只读）
                }
                else if (address >= 40001 && address <= 49999)// 40001 ~ 49999 03指令
                {
                    code = 3;// 读保存寄存器
                }
                var temp = Convert.ToUInt16(val.Substring(1));
                val = $"x={code};{temp}";
            }
        }
    }
}