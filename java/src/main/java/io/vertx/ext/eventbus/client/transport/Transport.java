package io.vertx.ext.eventbus.client.transport;

import io.netty.channel.ChannelInitializer;
import io.vertx.ext.eventbus.client.EventBusClientOptions;
import io.vertx.ext.eventbus.client.Handler;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public abstract class Transport extends ChannelInitializer {

  protected EventBusClientOptions options;

  Handler<Void> connectedHandler;
  Handler<String> messageHandler;
  Handler<Void> closeHandler;

  Transport(EventBusClientOptions options) {
    this.options = options;
  }

  public void connectedHandler(Handler<Void> handler) {
    connectedHandler = handler;
  }

  public void messageHandler(Handler<String> handler) {
    messageHandler = handler;
  }

  public void closeHandler(Handler<Void> handler) {
    closeHandler = handler;
  }

  public abstract void send(String message);

}
