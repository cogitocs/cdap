package io.cdap.cdap.internal.tether;

import java.util.Objects;
import javax.annotation.Nullable;

public class PeerInfo {
  private String name;
  @Nullable
  private String endpoint;
  private TetherStatus tetherStatus;
  private PeerMetadata peerMetadata;

  public PeerInfo(String name, @Nullable String endpoint, TetherStatus tetherStatus, PeerMetadata peerMetadata) {
    this.name = name;
    this.endpoint = endpoint;
    this.tetherStatus = tetherStatus;
    this.peerMetadata = peerMetadata;
  }

  public String getName() {
    return name;
  }

  @Nullable
  public String getEndpoint() {
    return endpoint;
  }

  public TetherStatus getTetherStatus() {
    return tetherStatus;
  }

  public PeerMetadata getPeerMetadata() {
    return peerMetadata;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    PeerInfo that = (PeerInfo) other;
    return Objects.equals(this.name, that.name) &&
      Objects.equals(this.endpoint, that.endpoint) &&
      Objects.equals(this.tetherStatus, that.tetherStatus) &&
      Objects.equals(this.peerMetadata, that.peerMetadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, endpoint, tetherStatus, peerMetadata);
  }
}
