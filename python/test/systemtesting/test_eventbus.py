import unittest
from testeventbus import EventBusBridgeStarter, CountDownLatch
from vertx import EventBus


class EventBusClientTests(unittest.TestCase):
    """
    This is the tests against a local test eventbus bridge
    """

    starter = None

    def __init__(self, *args, **kwargs):
        super(EventBusClientTests, self).__init__(*args, **kwargs)

    @classmethod
    def setUpClass(cls):
        cls.starter = EventBusBridgeStarter(debug=True)
        cls.starter.start()
        cls.starter.wait_started()

    @classmethod
    def tearDownClass(cls):
        cls.starter.stop()

    def test_register_unregister(self):
        latch = CountDownLatch(2)
        ebus = EventBus(options={'connect': True})
    
        def list_handlers(message):
            print(message)
            handler_list = message['body']['list']
            addresses = [e['address'] for e in handler_list]
            if "first" in message['headers'] and message['headers']['first']:
                self.assertIn("list-handler", addresses)
                self.assertIn("test-handler", addresses)
                ebus.unregister_handler("test-handler")
                self.assertIn("list-handler", ebus.handlers)
                self.assertNotIn("test-handler", ebus.handlers)
                ebus.send("list", reply_address="list-handler")
                latch.count_down()
            else:
                self.assertIn("list-handler", addresses)
                self.assertNotIn("test-handler", addresses)
                ebus.close()
                latch.count_down()
            # check list handler again, no test-handler anymore !!

        ebus.register_handler("list-handler", list_handlers)
        ebus.register_handler("test-handler", lambda x: print(x))
        self.assertIn("list-handler", ebus.handlers)
        self.assertIn("test-handler", ebus.handlers)
        ebus.send("list", reply_address="list-handler", headers={'first': True})
        latch.awaits(5)

    def test_send(self):
        latch = CountDownLatch()
        ebus = EventBus()
        ebus.connect()
    
        def handler(message):
            self.assertEqual(message['body']['hello'], 'world')
            ebus.close()
            latch.count_down()
        ebus.register_handler("echo-back", handler)
        ebus.send("echo", reply_address="echo-back", body={'hello': 'world'})
        latch.awaits(5)

    def test_publish(self):
        latch = CountDownLatch()
        ebus = EventBus()
        ebus.connect()
    
        def handler(message):
            print("got publish messages back")
            self.assertEqual(message['body']['hello'], 'world')
            self.assertEqual(message['headers']['name'], 'vertx-python')
            ebus.close()
            latch.count_down()
        ebus.register_handler("publish-back", handler)
        ebus.publish("publish-back", headers={'name': 'vertx-python'}, body={'hello': 'world'})
        latch.awaits(5)


if __name__ == "__main__":
    unittest.main()
