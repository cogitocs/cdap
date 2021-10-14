package io.cdap.cdap.internal.tether;

import com.google.gson.JsonElement;

import java.util.Objects;
import javax.annotation.Nullable;

public class TetherControlMessage {
  public enum Type {
    KEEPALIVE,
    TETHER_ACCEPTED,
    TETHER_REJECTED,
    RUN_PIPELINE
  }
  private Type type;
  @Nullable
  private JsonElement payload;
  public TetherControlMessage(Type type, @Nullable JsonElement payload) {
    this.type = type;
    this.payload = payload;
  }

  public Type getType() {
    return type;
  }

  public JsonElement getPayload() {
    return payload;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TetherControlMessage that = (TetherControlMessage) o;
    return Objects.equals(type, that.type) &&
      Objects.equals(payload, that.payload);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, payload);
  }
}
