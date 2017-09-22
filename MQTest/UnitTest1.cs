using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RabbitMQ.Client;
using Newtonsoft.Json;
using System.Text;
using RabbitMQ.Client.Events;
using System.Threading;

namespace MQTest
{
    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        public void TestMethod1()
        {
            ServerSend();

            //var factory = new ConnectionFactory() { HostName = "localhost" };
            //using (var connection = factory.CreateConnection())
            //{
            //    using (var channel = connection.CreateModel())
            //    {
            //        channel.QueueDeclare(queue: "product",//指定发送消息的queue，和生产者的queue匹配
            //                             durable: false,
            //                             exclusive: false,
            //                             autoDelete: false,
            //                             arguments: null);

            //        var consumer = new EventingBasicConsumer(channel);
            //        //注册接收事件，一旦创建连接就去拉取消息
            //        consumer.Received += (model, ea) =>
            //        {
            //            Comsumer(model, ea);
            //        };
            //        channel.BasicConsume(queue: "product",
            //                             autoAck: false,//和tcp协议的ack一样，为false则服务端必须在收到客户端的回执（ack）后才能删除本条消息
            //                             consumer: consumer);
            //        Thread.Sleep(10000);
            //    }
            //}
            Thread.Sleep(1000);
        }

        public static void ManualAckReceive(IModel channel)
        {
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                var body = ea.Body;
                var message = Encoding.UTF8.GetString(body);
                Console.WriteLine(" [x] Received {0}", message);

                int dots = message.Split('.').Length - 1;
                Thread.Sleep(dots * 1000);

                Console.WriteLine(" [x] Done");

                channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
            };
            channel.BasicConsume(queue: "task_queue", autoAck: false, consumer: consumer);
        }


        public static void Comsumer(object model, BasicDeliverEventArgs ea)
        {
            var body = ea.Body;
            var message = Encoding.UTF8.GetString(body);
            var product = JsonConvert.DeserializeObject<Product>(message);

            Assert.IsTrue(product.Name.Contains("Product"));
            //Console.WriteLine(" [x] Received {0}", message);
            //Console.WriteLine($"Name : {product.Name} , Price : {product.Price}");
        }

        public static void ServerSend()
        {
            //创建代理服务器实例。注意：HostName为Rabbit Server所在服务器的ip或域名，如果服务装在本机，则为localhost，默认端口5672
            var factory = new ConnectionFactory() { HostName = "localhost", UserName="guest", Password="guest0000"};
            //创建socket连接
            using (var connection = factory.CreateConnection())
            {
                //channel中包含几乎所有的api来供我们操作queue
                using (var channel = connection.CreateModel())
                {
                    //声明queue
                    channel.QueueDeclare(queue: "product",//队列名
                                         durable: false,//是否持久化
                                         exclusive: false,//排它性
                                         autoDelete: false,//一旦客户端连接断开则自动删除queue
                                         arguments: null);//如果安装了队列优先级插件则可以设置优先级
           
                    for (int i = 0; i < 5; i++)
                    {
                        var product = new Product() { Name = "Product_" + i.ToString(), Price = 1000 * i };
                        var message = JsonConvert.SerializeObject(product);
                        var body = Encoding.UTF8.GetBytes(message);
                        
                        channel.BasicPublish(exchange: "",//exchange名称
                                        routingKey: "Product",//如果存在exchange,则消息被发送到名称为hello的queue的客户端
                                        basicProperties: new RabbitMQ.Client.Framing.BasicProperties { Persistent = true },
                                        body: body);//消息体
                        //Console.WriteLine(" [x] Sent {0}", message);
                        //Thread.Sleep(1000);
                    }
                }
            }
            //Console.WriteLine(" Press [enter] to exit.");
            //Console.ReadLine();
        } 

        public class Product
        {
            public string Name { get; set; }
            public int Price { get; set; }
        }
    }
}
