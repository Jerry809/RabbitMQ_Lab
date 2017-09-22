using Newtonsoft.Json;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MQServer
{
    class Program
    {
        static void Main(string[] args)
        {
            var routingKey = "Hello";
            var exchangeName = "direct-exchange";

            //创建代理服务器实例。注意：HostName为Rabbit Server所在服务器的ip或域名，如果服务装在本机，则为localhost，默认端口5672
            var factory = new ConnectionFactory() { HostName = "localhost", UserName = "guest", Password = "guest0000" };
            //创建socket连接
            using (var connection = factory.CreateConnection())
            {
                //channel中包含几乎所有的api来供我们操作queue
                using (var channel = connection.CreateModel())
                {
                    channel.ExchangeDeclare(exchangeName, ExchangeType.Direct, true, false, null);

                    //声明queue
                    var queueOk = channel.QueueDeclare(queue: routingKey,//队列名
                                          durable: true,//是否持久化
                                          exclusive: false,//排它性
                                          autoDelete: false,//一旦客户端连接断开则自动删除queue
                                          arguments: null);//如果安装了队列优先级插件则可以设置优先级
                    channel.QueueBind(queueOk.QueueName, exchangeName, routingKey);

                    for (int i = 0; i < 5; i++)
                    {
                        var product = new Product() { Name = "Product_" + i.ToString(), Price = 1000 * i };
                        var message = JsonConvert.SerializeObject(product);
                        var body = Encoding.UTF8.GetBytes(message);

                        channel.BasicPublish(exchange: exchangeName,//exchange名称
                                        routingKey: routingKey,//如果存在exchange,则消息被发送到名称为hello的queue的客户端
                                        basicProperties: new RabbitMQ.Client.Framing.BasicProperties { Persistent = true },
                                        body: body);//消息体
                        Console.WriteLine(" [x] Sent {0}", message);
                        //Thread.Sleep(1000);
                    }
                }
            }
            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }
    }

    public class Product
    {
        public string Name { get; set; }
        public int Price { get; set; }
    }
}
