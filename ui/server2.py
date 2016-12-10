###############################################################################
#
# The MIT License (MIT)
#
# Copyright (c) Tavendo GmbH
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
###############################################################################

from autobahn.asyncio.websocket import WebSocketServerProtocol, \
    WebSocketServerFactory
from pyhive import hive
from TCLIService.ttypes import TOperationState

cursor = hive.connect('localhost').cursor()

class MyServerProtocol(WebSocketServerProtocol):
    IsSending = False

    def onConnect(self, request):
        print("Client connecting: {0}".format(request.peer))

    def onOpen(self):
        print("WebSocket connection open.")

    def onMessage(self, payload, isBinary):
        data = payload.decode('utf8')
        if data.startswith('review:'):
            print("enter to review part")

            v = data.split(":")
            hql = ("SELECT data FROM negative_review where business_id = %(bid)s")
            hql_data = {'bid': v[1]}
            cursor.execute(hql, hql_data)
            neg_reviews = cursor.fetchall()


            print("==============getting The neg reviews")
            #print(neg_review_id)
            #print(neg_reviews)

            self.sendMessage('negative', isBinary = False)

            print("negative signal get sent-----------------------")
            for review in neg_reviews:
                print(review[0])
                self.sendMessage(review[0].encode('utf8'), isBinary = False)

            print("wait positive----------------------------------")
            hql = ("SELECT data FROM positive_review where business_id = %(bid)s")
            hql_data = {'bid': v[1]}
            cursor.execute(hql, hql_data)
            pos_reviews = cursor.fetchall()

            print("-----here------")

            self.sendMessage('positive', isBinary = False)

            #print(pos_reviews + "------------")
            print("positive signal get sent-----------------------")
            for review in pos_reviews:
                self.sendMessage(review[0].encode('utf8'), isBinary = False)
            print("positive sent ------------")


        else:
            v = data.split(",")
            hql = ("SELECT business_id FROM searchindex where "
                "latitude >= %(latitudeMin)s and latitude <= %(latitudeMax)s and "
                "longitude >= %(longitudeMin)s and longitude <= %(longitudeMax)s limit 10")
            hql_data = {
            'latitudeMin': v[0],
            'latitudeMax': v[1],
            'longitudeMin': v[2],
            'longitudeMax': v[3],
            'month': "avg_stars_month_" + v[4],
            }
            cursor.execute(hql, hql_data)
            bids = cursor.fetchall()
            print("wait data")
            #print(bids)

            for bid in bids:
                print ("get data " + bid[0])
                hql = ("SELECT detail FROM BusinessIndex where business_id = %(business_id)s")
                hql_data = {'business_id': bid[0]}
                cursor.execute(hql, hql_data)
                detail = cursor.fetchone()
                self.sendMessage(detail[0].encode('utf8'), isBinary = False)



    def onClose(self, wasClean, code, reason):
        print("WebSocket connection closed: {0}".format(reason))


if __name__ == '__main__':

    try:
        import asyncio
    except ImportError:
        # Trollius >= 0.3 was renamed
        import trollius as asyncio

    #factory = WebSocketServerFactory(u"ws://192.168.56.2:9998")
	factory = WebSocketServerFactory(u"ws://0.0.0.0:9998")
    factory.protocol = MyServerProtocol

    loop = asyncio.get_event_loop()
    coro = loop.create_server(factory, '0.0.0.0', 9998)
    server = loop.run_until_complete(coro)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.close()
        loop.close()