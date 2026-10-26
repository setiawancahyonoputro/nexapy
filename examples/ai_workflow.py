from nexapy import NexaPy, workflow

app = NexaPy(title="AI Workflow Demo")


@workflow("summarize", description="Summarize provided article content using AI Action")
async def summarize_workflow(ctx):
    article = ctx.input.get("article", "No text provided.")
    response = await ctx.ai(f"Summarize this text concisely: {article}")
    return {
        "summary": response.text,
        "provider": response.provider,
    }


# For development/demo, allow_anonymous=True is explicitly set.
# In production, provide api_key="your-api-key" or hmac_secret="your-hmac-secret".
app.mount_workflows(allow_anonymous=True)
