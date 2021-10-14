package io.cdap.cdap.internal.tether;

import com.google.common.net.HttpHeaders;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteAuthenticator;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HandlerContext;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

@Path(Constants.Gateway.API_VERSION_3)
public class TetherClientHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(TetherClientHandler.class);
  private static final Gson GSON = new Gson();


  private final Map<String, Long> controlChannels = new ConcurrentHashMap<>();
  private final CConfiguration cConf;
  private final TetherStore store;
  public final String CREATE_TETHER = "/v3/tethering/create";
  public final String CONNECT_CONTROL_CHANNEL = "/v3/tethering/controlchannels";
  public final List<PeerInfo> peers;
  private int connectionTimeout;
  private ScheduledExecutorService scheduledExecutorService;
  private RemoteAuthenticator authenticator;

  @Inject
  TetherClientHandler(CConfiguration cConf, TetherStore store) {
    this.cConf = cConf;
    this.store = store;
    this.peers = store.getPeers();
  }

  public void init(HandlerContext context) {
    int connectionInterval = cConf.getInt(Constants.Tether.CONNECT_INTERVAL,
                                          Constants.Tether.CONNECT_INTERVAL_DEFAULT);
    connectionTimeout = cConf.getInt(Constants.Tether.CONNECTION_TIMEOUT,
                                     Constants.Tether.CONNECTION_TIMEOUT_DEFAULT);
    scheduledExecutorService = Executors
      .newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("tether-control-channel"));
    scheduledExecutorService.scheduleAtFixedRate(
      () -> {
        // work on a copy of peers to avoid synchronization issues.
        for (PeerInfo peer : new ArrayList<>(peers)) {
          if (peer.getEndpoint() == null) {
            // should not happen
            LOG.error("Peer {} does not have endpoint set", peer.getName());
            continue;
          }
          try {
            HttpResponse resp = sendHttpRequest(HttpMethod.PUT, new URI(peer.getEndpoint()).resolve(CONNECT_CONTROL_CHANNEL), null);
            if (resp.getResponseCode() < 500) {
              controlChannels.put(peer.getName(), System.currentTimeMillis());
              processTetherControlMessage(resp.getResponseBodyAsString(StandardCharsets.UTF_8), peer);
            } else {
              LOG.error("Peer {} returned error code {} body {}", peer.getName(), resp.getResponseCode(),
                        resp.getResponseBodyAsString());
            }
          } catch (IOException | URISyntaxException e) {
            LOG.debug("Failed to create control channel to {}", peer, e);
          }
        }
      }, 0, connectionInterval, TimeUnit.SECONDS);
  }

  public void destroy(HandlerContext context) {
    scheduledExecutorService.shutdown();
  }

  /**
   * Returns status of control channels.
   */
  @GET
  @Path("/tethering/controlchannels")
  public void getControlChannels(HttpRequest request, HttpResponder responder) {
    Type type = new TypeToken<Map<String, String>>() {
    }.getType();
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
   * Returns a list of tethering requests.
   */
  @GET
  @Path("/tethering/requests")
  public void getTethers(HttpRequest request, HttpResponder responder) {
    synchronized (this) {
      List<PeerInfo> peers = store.getPeers();
    }
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(peers));
  }

  /**
   * Initiates tethering with the server.
   */
  @POST
  @Path("/tethering/create")
  public void createTether(FullHttpRequest request, HttpResponder responder) throws IOException {
    String content = request.content().toString(StandardCharsets.UTF_8);
    TetherRequest tetherRequest = GSON.fromJson(content, TetherRequest.class);
    URI endpoint = tetherRequest.getEndpoint();

    HttpResponse response = sendHttpRequest(HttpMethod.POST, endpoint.resolve(CREATE_TETHER),
                                                        content);
    if (response.getResponseCode() != 200) {
      LOG.error("Failed to send tether request, body: {}, code: {}",
                response.getResponseBody(), response.getResponseCode());
      responder.sendStatus(HttpResponseStatus.valueOf(response.getResponseCode()));
      return;
    }

    PeerMetadata peerMetadata = new PeerMetadata(tetherRequest.getProject(), tetherRequest.getLocation(),
                                                 tetherRequest.getNamespaces());
    PeerInfo peerInfo = new PeerInfo(tetherRequest.getInstance(), endpoint.toString(),
                                     TetherStatus.PENDING, peerMetadata);
    synchronized (this) {
      store.addPeer(peerInfo);
      peers.add(peerInfo);
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

  private synchronized void processTetherControlMessage(String message, PeerInfo peerInfo) {
    TetherControlMessage tetherControlMessage = GSON.fromJson(message, TetherControlMessage.class);
    switch (tetherControlMessage.getType()) {
      case KEEPALIVE:
        LOG.debug("Got keeplive from {}", peerInfo.getName());
        break;
      case TETHER_ACCEPTED:
        LOG.debug("Peer {} accepted tethering", peerInfo.getName());
        if (store.getStatus(peerInfo.getName()) != TetherStatus.PENDING) {
          LOG.info("Ignoring TETHER_ACCEPTED message from {}, current state: {}", peerInfo.getName(),
                   store.getStatus(peerInfo.getName()));
        }
        store.updatePeer(peerInfo, TetherStatus.ACCEPTED);
        break;
      case TETHER_REJECTED:
        LOG.debug("Peer {} rejected tethering", peerInfo.getName());
        if (store.getStatus(peerInfo.getName()) != TetherStatus.PENDING) {
          LOG.info("Ignoring TETHER_REJECTED message from {}, current state: {}", peerInfo.getName(),
                   store.getStatus(peerInfo.getName()));
        }
        store.updatePeer(peerInfo, TetherStatus.REJECTED);
        break;
      case RUN_PIPELINE:
        LOG.debug("Peer {} rejected tethering", peerInfo.getName());
        // TODO: add processing logic
        break;
    }
  }

  public HttpResponse sendHttpRequest(HttpMethod method, URI endpoint, @Nullable String content)
    throws IOException {
    io.cdap.common.http.HttpRequest.Builder builder;
    switch (method) {
      case GET:
        builder = io.cdap.common.http.HttpRequest.get(endpoint.toURL());
        break;
      case PUT:
        builder = io.cdap.common.http.HttpRequest.put(endpoint.toURL());
        break;
      case POST:
        builder = io.cdap.common.http.HttpRequest.post(endpoint.toURL());
        break;
      case DELETE:
        builder = io.cdap.common.http.HttpRequest.delete(endpoint.toURL());
        break;
      default:
        throw new RuntimeException("Unexpected HTTP method: " + method);
    }
    if (content != null) {
      builder.withBody(content);
    }
    // Add Authorization header.
    RemoteAuthenticator authenticator = getAuthenticator();
    if (authenticator != null) {
      builder.addHeader(HttpHeaders.AUTHORIZATION,
                        String.format("%s %s", authenticator.getType(), authenticator.getCredentials()));
    }
    return HttpRequests.execute(builder.build());
  }

  @Nullable
  private RemoteAuthenticator getAuthenticator() {
    RemoteAuthenticator authenticator = this.authenticator;
    if (authenticator != null) {
      return authenticator;
    }
    // No need to synchronize as the get default method is thread safe and we don't need a singleton for authenticator
    this.authenticator = authenticator = RemoteAuthenticator.getDefaultAuthenticator();
    return authenticator;
  }
}
