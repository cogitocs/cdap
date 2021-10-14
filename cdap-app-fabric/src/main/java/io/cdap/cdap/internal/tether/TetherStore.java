package io.cdap.cdap.internal.tether;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TetherStore {
  private static final Gson GSON = new GsonBuilder().create();

  private final TransactionRunner transactionRunner;

  @Inject
  TetherStore(TransactionRunner transactionRunner) {
    this.transactionRunner = transactionRunner;
  }

  public void addPeer(PeerInfo peerInfo) {
    updatePeer(peerInfo, peerInfo.getTetherStatus());
  }

  public void updatePeer(PeerInfo peerInfo, TetherStatus tetherStatus) {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable tetherTable = context.getTable(StoreDefinition.TetherStore.TETHER);
      Collection<Field<?>> fields = new ArrayList<>();
      fields.add(Fields.stringField(StoreDefinition.TetherStore.PEER_NAME_FIELD, peerInfo.getName()));
      fields.add(Fields.stringField(StoreDefinition.TetherStore.PEER_URI_FIELD, peerInfo.getEndpoint()));
      fields.add(Fields.stringField(StoreDefinition.TetherStore.STATE_FIELD, tetherStatus.toString()));
      fields.add(Fields.stringField(StoreDefinition.TetherStore.PEER_METADATA_FIELD,
                                    GSON.toJson(peerInfo.getPeerMetadata())));

      tetherTable.upsert(fields);
    });
  }

  public void deletePeer(String peer_name) {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable capabilityTable = context.getTable(StoreDefinition.TetherStore.TETHER);
      capabilityTable
        .delete(Collections.singleton(Fields.stringField(StoreDefinition.TetherStore.PEER_NAME_FIELD, peer_name)));
    });
  }

  List<PeerInfo> getPeers() {
    List<PeerInfo> peers = new ArrayList<>();
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable tetherTable = context
        .getTable(StoreDefinition.TetherStore.TETHER);
      CloseableIterator<StructuredRow> iterator = tetherTable
        .scan(Range.all(), Integer.MAX_VALUE);
      iterator.forEachRemaining(row -> {
        String peer_name = row.getString(StoreDefinition.TetherStore.PEER_NAME_FIELD);
        String endpoint = row.getString(StoreDefinition.TetherStore.PEER_URI_FIELD);
        TetherStatus tetherStatus = TetherStatus.valueOf(row.getString(StoreDefinition.TetherStore.STATE_FIELD));
        PeerMetadata peerMetadata = GSON.fromJson(row.getString(StoreDefinition.TetherStore.PEER_METADATA_FIELD),
                                                  PeerMetadata.class);
        peers.add(new PeerInfo(peer_name, endpoint, tetherStatus, peerMetadata));
      });
      return peers;
    });
  }

  PeerInfo getPeer(String peerName) {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable tetherTable = context
        .getTable(StoreDefinition.TetherStore.TETHER);
      Range range =  Range.singleton(
        ImmutableList.of(Fields.stringField(StoreDefinition.TetherStore.PEER_NAME_FIELD, peerName)));
      try (CloseableIterator<StructuredRow> iterator = tetherTable.scan(range, Integer.MAX_VALUE)) {
        if (!iterator.hasNext()) {
          throw new NotFoundException(peerName);
        }
        StructuredRow row = iterator.next();
        String endpoint = row.getString(StoreDefinition.TetherStore.PEER_URI_FIELD);
        TetherStatus tetherStatus = TetherStatus.valueOf(row.getString(StoreDefinition.TetherStore.STATE_FIELD));
        PeerMetadata peerMetadata = GSON.fromJson(row.getString(StoreDefinition.TetherStore.PEER_METADATA_FIELD),
                                                  PeerMetadata.class);
        return new PeerInfo(peerName, endpoint, tetherStatus, peerMetadata);

      }
    });
  }

  TetherStatus getStatus(String peer_name) {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable tetherTable = context
        .getTable(StoreDefinition.TetherStore.TETHER);
      Range range =  Range.singleton(
        ImmutableList.of(Fields.stringField(StoreDefinition.TetherStore.PEER_NAME_FIELD, peer_name)));
      try (CloseableIterator<StructuredRow> iterator = tetherTable.scan(range, Integer.MAX_VALUE)) {
        if (!iterator.hasNext()) {
          throw new NotFoundException(peer_name);
        }
        return TetherStatus.valueOf(iterator.next().getString(StoreDefinition.TetherStore.STATE_FIELD));
      }
    });
  }

  public void setMessageId(String topic, String messageId) {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable tetherTable = context.getTable(StoreDefinition.TetherStore.TETHER_EVENTS);
      Collection<Field<?>> fields = new ArrayList<>();
      fields.add(Fields.stringField(StoreDefinition.TetherStore.TOPIC_FIELD, topic));
      fields.add(Fields.stringField(StoreDefinition.TetherStore.MESSAGE_ID_FIELD, messageId));
      tetherTable.upsert(fields);
    });
  }

  public Map<String, String> getMessageIds() {
    Map<String, String> messageIds = new HashMap();
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable tetherTable = context
        .getTable(StoreDefinition.TetherStore.TETHER_EVENTS);
      try (CloseableIterator<StructuredRow> iterator = tetherTable.scan(Range.all(), Integer.MAX_VALUE)) {
        while (iterator.hasNext()) {
          StructuredRow row = iterator.next();
          String topic = row.getString(StoreDefinition.TetherStore.TOPIC_FIELD);
          String messageId = row.getString(StoreDefinition.TetherStore.MESSAGE_ID_FIELD);
          messageIds.put(topic, messageId);
        }
        return messageIds;
      }
    });
  }
}
