package io.cdap.cdap.internal.tether;

import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

public class PeerMetadata {
  @Nullable
  private final String project;
  @Nullable
  private final String location;
  private final List<NamespaceAllocation> namespaces;

  public PeerMetadata(@Nullable String project, @Nullable String location, List<NamespaceAllocation> namespaces) {
    this.project = project;
    this.location = location;
    this.namespaces = namespaces;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    PeerMetadata that = (PeerMetadata) other;
    return Objects.equals(this.project, that.project) &&
      Objects.equals(this.location, that.location) &&
      Objects.equals(this.namespaces, that.namespaces);
  }

  @Override
  public int hashCode() {
    return Objects.hash(project, location, namespaces);
  }
}
