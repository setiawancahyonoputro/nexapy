import React, { useState } from "react";
import { useNexaPy } from "../nexapy_sdk/react";

export const SupportApp: React.FC = () => {
  const [question, setQuestion] = useState("");
  const [reply, setReply] = useState("");
  const { chat, loading, error } = useNexaPy({
    baseUrl: "http://localhost:8000",
  });

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!question.trim()) return;

    try {
      // Direct call to NexaPy Workflow API or AI Completion
      const response = await fetch("http://localhost:8000/workflows/customer-support", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ id: 1, question }),
      });

      const data = await response.json();
      if (data.success) {
        setReply(data.result.answer);
      }
    } catch (err) {
      console.error("Workflow execution failed:", err);
    }
  };

  return (
    <div style={{ maxWidth: 600, margin: "40px auto", fontFamily: "sans-serif" }}>
      <h2>NexaPy AI Customer Support</h2>
      <form onSubmit={handleSubmit}>
        <textarea
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
          placeholder="Tanyakan sesuatu tentang akun Anda..."
          rows={4}
          style={{ width: "100%", padding: 12, borderRadius: 8, border: "1px solid #ccc" }}
        />
        <button
          type="submit"
          disabled={loading}
          style={{
            marginTop: 12,
            padding: "10px 20px",
            background: "#4f46e5",
            color: "#fff",
            border: "none",
            borderRadius: 6,
            cursor: "pointer",
          }}
        >
          {loading ? "Processing..." : "Send Request"}
        </button>
      </form>

      {reply && (
        <div style={{ marginTop: 24, padding: 16, background: "#f3f4f6", borderRadius: 8 }}>
          <strong>Response:</strong>
          <p>{reply}</p>
        </div>
      )}
    </div>
  );
};
