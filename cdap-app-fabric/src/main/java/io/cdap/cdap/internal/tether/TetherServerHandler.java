package io.cdap.cdap.internal.tether;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.api.messaging.MessageFetcher;
import io.cdap.cdap.api.messaging.MessagePublisher;
import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HandlerContext;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

@Path(Constants.Gateway.API_VERSION_3)
public class TetherServerHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(TetherServerHandler.class);
  private static final Gson GSON = new GsonBuilder().create();
  private static final String TETHERING_TOPIC_PREFIX = "tethering_";
  private final Map<String, Long> controlChannels = new ConcurrentHashMap<>();
  private final CConfiguration cConf;
  private final TetherStore store;
  private final MessagingService messagingService;
  private final MultiThreadMessagingContext messagingContext;
  // Connection timeout in seconds.
  private int connectionTimeout;
  // Last processed message id for each topic. There's a separate topic for each peer.
  private Map<String, String> lastMessageIds;

  @Inject
  TetherServerHandler(CConfiguration cConf, TetherStore store, MessagingService messagingService) {
    this.cConf = cConf;
    this.store = store;
    this.messagingService = messagingService;
    this.messagingContext = new MultiThreadMessagingContext(messagingService);
  }

  @Override
  public void init(HandlerContext context) {
    super.init(context);
    lastMessageIds = store.getMessageIds();
    connectionTimeout = cConf.getInt(Constants.Tether.CONNECTION_TIMEOUT, Constants.Tether.CONNECTION_TIMEOUT_DEFAULT);
  }

  /**
   * Returns status of control channels.
   */
  @GET
  @Path("/tethering/controlchannels")
  public void getControlChannels(HttpRequest request, HttpResponder responder) {
    Type type = new TypeToken<Map<String, String>>() { }.getType();
    Map<String, TetherConnectionStatus> channelStatus = new HashMap<>();
    Long now = System.currentTimeMillis();
    for (Map.Entry<String, Long> entry : controlChannels.entrySet()) {
      if (now - entry.getValue() < connectionTimeout * 1000L) {
        channelStatus.put(entry.getKey(), TetherConnectionStatus.ACTIVE);
      } else {
        channelStatus.put(entry.getKey(), TetherConnectionStatus.INACTIVE);
      }
    }
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(channelStatus, type));
  }

  /**
   * Sends control commands to the client.
   */
  @PUT
  @Path("/tethering/controlchannels/{peer}")
  public void connectControlChannel(HttpRequest request, HttpResponder responder,
                                    @PathParam("peer") String peer) throws IOException {
    controlChannels.put(peer, System.currentTimeMillis());
    List<TetherControlMessage> commands = new ArrayList<>();

    MessageFetcher fetcher = messagingContext.getMessageFetcher();
    TopicId topic = new TopicId(NamespaceId.SYSTEM.getNamespace(), TETHERING_TOPIC_PREFIX + peer);
    String lastMessageId = null;
    try (CloseableIterator<Message> iterator =
           fetcher.fetch(topic.getNamespace(), topic.getTopic(), 1, lastMessageIds.get(topic.getTopic()))) {
      while (iterator.hasNext()) {
        Message message = iterator.next();
        TetherControlMessage controlMessage = GSON.fromJson(message.getPayloadAsString(StandardCharsets.UTF_8),
                                                            TetherControlMessage.class);
        commands.add(controlMessage);
        lastMessageId = message.getId();
      }
    } catch (TopicNotFoundException e) {
      LOG.warn("Received control connection from peer {} that's not tethered", peer);
    }

    if (lastMessageId != null) {
      // Update the last message id for the topic if we read any messages
      lastMessageIds.put(topic.getTopic(), lastMessageId);
      synchronized (this) {
        store.setMessageId(topic.getTopic(), lastMessageId);
      }
    }

    if (commands.isEmpty()) {
      commands.add(new TetherControlMessage(TetherControlMessage.Type.KEEPALIVE, null));
    }

    Type type = new TypeToken<List<TetherControlMessage>>() { }.getType();
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(commands, type));
  }

  /**
   * Creates a tether with a client.
   */
  @POST
  @Path("/tethering/create")
  public void createTether(FullHttpRequest request, HttpResponder responder) throws IOException {
    String content = request.content().toString(StandardCharsets.UTF_8);
    TetherRequest tetherRequest = GSON.fromJson(content, TetherRequest.class);

    // We don't need to keep track of the client's project, location on the server side.
    PeerMetadata peerMetadata = new PeerMetadata(null, null, tetherRequest.getNamespaces());
    // We don't store the peer endpoint on the server side because the connection is initiated by the client.
    PeerInfo peerInfo = new PeerInfo(tetherRequest.getInstance(), null, TetherStatus.PENDING, peerMetadata);
    synchronized (this) {
      store.addPeer(peerInfo);
    }
    TopicId topicId = new TopicId(NamespaceId.SYSTEM.getNamespace(), "tethering_" + tetherRequest.getInstance());
    try {
      messagingService.createTopic(new TopicMetadata(topicId, Collections.emptyMap()));
    } catch (TopicAlreadyExistsException | IOException e) {
      LOG.warn("Topic {} already exists", topicId);
    }
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Returns a list of tethering requests.
   */
  @GET
  @Path("/tethering/requests")
  public void getTethers(HttpRequest request, HttpResponder responder) {
    List<PeerInfo> peers;
    synchronized (this) {
      peers = store.getPeers();
    }
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(peers));
  }

  /**
   * Accepts the tethering request.
   */
  @POST
  @Path("/tethering/requests/{peer}/accept")
  public void acceptTether(HttpRequest request, HttpResponder responder, @PathParam("peer") String peer) {
    synchronized (this) {
      PeerInfo peerInfo = store.getPeer(peer);
      if (peerInfo.getTetherStatus() == TetherStatus.PENDING) {
        store.updatePeer(peerInfo, TetherStatus.ACCEPTED);
        publishMessage(new TopicId(NamespaceId.SYSTEM.getNamespace(), TETHERING_TOPIC_PREFIX + peer),
                       new TetherControlMessage(TetherControlMessage.Type.TETHER_ACCEPTED, null));
      } else {
        LOG.info("Ignoring tethering accept as tethering state is {}", peerInfo.getTetherStatus());
      }
    }
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Rejects the tethering request.
   */
  @POST
  @Path("/tethering/requests/{peer}/reject")
  public void rejectTether(HttpRequest request, HttpResponder responder, @PathParam("peer") String peer) {
    synchronized (this) {
      PeerInfo peerInfo = store.getPeer(peer);
      if (peerInfo.getTetherStatus() == TetherStatus.PENDING) {
        store.updatePeer(peerInfo, TetherStatus.REJECTED);
        publishMessage(new TopicId(NamespaceId.SYSTEM.getNamespace(), TETHERING_TOPIC_PREFIX + peer),
                       new TetherControlMessage(TetherControlMessage.Type.TETHER_REJECTED, null));
      } else {
        LOG.info("Ignoring tethering reject as tethering state is {}", peerInfo.getTetherStatus());
      }
    }
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Deletes a tether.
   */
  @DELETE
  @Path("/tethering/connections/{peer}")
  public void deleteTether(HttpRequest request, HttpResponder responder, @PathParam("peer") String peer) {
    synchronized (this) {
      store.deletePeer(peer);
      controlChannels.remove(peer);
    }
    responder.sendStatus(HttpResponseStatus.OK);
  }

  private void publishMessage(TopicId topicId,TetherControlMessage message) {
    MessagePublisher publisher = messagingContext.getMessagePublisher();
    try {
      publisher.publish(topicId.getNamespace(), topicId.getTopic(), StandardCharsets.UTF_8,
                        GSON.toJson(message));
    } catch (TopicNotFoundException e) {
      // should not happen because we created the topic earlier.
      LOG.error("Topic {} was not found", topicId);
    } catch (IOException e) {
      LOG.error("Failed to publish to topic {}", topicId, e);
    }
  }
}
