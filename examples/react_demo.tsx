import React from "react";
import { useNexaPy } from "../nexapy_sdk/react/useNexaPy";

export function ChatComponent() {
  const { send, data, loading, error, reset } = useNexaPy();

  const handleAsk = () => {
    send("Apa itu NexaPy Framework?");
  };

  return (
    <div style={{ padding: "20px", fontFamily: "sans-serif" }}>
      <h2>NexaPy React Demo</h2>

      <button onClick={handleAsk} disabled={loading}>
        {loading ? "Thinking..." : "Ask NexaPy"}
      </button>

      <button onClick={reset} style={{ marginLeft: "10px" }}>
        Reset
      </button>

      {error && (
        <div style={{ color: "red", marginTop: "10px" }}>
          Error: {error}
        </div>
      )}

      {data && data.success && (
        <div style={{ marginTop: "15px", background: "#f0f0f0", padding: "10px", borderRadius: "5px" }}>
          <p><strong>Response:</strong> {data.text}</p>
          <small>Provider: {data.provider} | Model: {data.model}</small>
        </div>
      )}
    </div>
  );
}
