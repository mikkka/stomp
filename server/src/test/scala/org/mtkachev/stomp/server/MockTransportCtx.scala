package org.mtkachev.stomp.server

import codec.Frame


class MockTransportCtx extends TransportCtx {
  def write(msg: Frame) {}
  def isClosing = false
  def close() {}
  def setSubscriber(subscriber: Subscriber) {}
}