from nexapy import NexaPy, workflow

app = NexaPy(title="HTTP Workflow Demo")


@workflow("fetch-user", description="Fetch user profile from external API")
async def fetch_user(ctx):
    user_id = ctx.input.get("id", 1)
    res = await ctx.http.get(f"https://jsonplaceholder.typicode.com/users/{user_id}")
    return {
        "status": res.status_code,
        "data": res.json(),
    }


# For development/demo, allow_anonymous=True is explicitly set.
# In production, provide api_key="your-api-key" or hmac_secret="your-hmac-secret".
app.mount_workflows(allow_anonymous=True)
