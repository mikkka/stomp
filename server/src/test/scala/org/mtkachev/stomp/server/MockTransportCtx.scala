package org.mtkachev.stomp.server

import org.mtkachev.stomp.codec.Frame


class MockTransportCtx extends TransportCtx {
  def write(msg: Frame) {}
  def isClosing = false
  def close() {}
  def setSubscriber(subscriber: Subscriber) {}
}