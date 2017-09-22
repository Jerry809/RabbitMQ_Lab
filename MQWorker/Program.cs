using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MQWorker
{
    class Program
    {
        static void Main(string[] args)
        {
            var routingKey = "Hello";
            var exchangeName = "direct-exchange";

            var factory = new ConnectionFactory() { HostName = "localhost", UserName = "guest", Password = "guest0000" };
            using (
                var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.QueueDeclare(queue: routingKey,//指定发送消息的queue，和生产者的queue匹配
                                         durable: true,
                                         exclusive: false,
                                         autoDelete: false,
                                         arguments: null);
                    channel.BasicQos(0, 1, false);

                    ManualAckReceive(channel);
                    Console.WriteLine(" Press [enter] to exit.");
                    Console.ReadLine();
                }
            }   
        }

        public static void AutoAckReceive(IModel channel)
        {
            var consumer = new EventingBasicConsumer(channel);
            //注册接收事件，一旦创建连接就去拉取消息
            consumer.Received += (model, ea) =>
            {
                var body = ea.Body;
                var message = Encoding.UTF8.GetString(body);
                var product = JsonConvert.DeserializeObject<Product>(message);
                Console.WriteLine(" [x] Received {0}", message);
                Console.WriteLine($"Name : {product.Name} , Price : {product.Price}");
            };
            channel.BasicConsume(queue: "product",
                                 autoAck: true,//和tcp协议的ack一样，为false则服务端必须在收到客户端的回执（ack）后才能删除本条消息
                                 consumer: consumer);
            Thread.Sleep(1000);

        }

        public static void ManualAckReceive(IModel channel)
        {
            var routingKey = "Hello";
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                var body = ea.Body;
                var message = Encoding.UTF8.GetString(body);
                var product = JsonConvert.DeserializeObject<Product>(message);
                Console.WriteLine(" [x] Received {0}", message);
                Console.WriteLine($"Name:{product.Name} , price:{product.Price}");

                for (int i = 0; i < 5; i++)
                {
                    Console.WriteLine($"{i} second");
                    Thread.Sleep(1000);
                }
                Console.WriteLine($"messageId: {ea.BasicProperties.AppId}");
                Console.WriteLine(" [x] Done");
                //重新丟回Queue裡, 但是是丟回原本的位置
                //channel.BasicNack(deliveryTag: ea.DeliveryTag, multiple: false, requeue: true);
                
                //channel.BasicAck(ea.DeliveryTag, false);
                //channel.BasicPublish(exchange: "",//exchange名称
                //                        routingKey: "product",//如果存在exchange,则消息被发送到名称为hello的queue的客户端
                //                        basicProperties: new RabbitMQ.Client.Framing.BasicProperties { Persistent = true },
                //                        body: body);

                //完成回傳Ack , 讓Queue刪掉工作
                channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                
            };
            channel.BasicConsume(queue: routingKey, autoAck: false, consumer: consumer);
            //Thread.Sleep(1000);
            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }

        public class Product
        {
            public string Name { get; set; }
            public int Price { get; set; }
        }
    }
}
