from nexapy import NexaPy, workflow

app = NexaPy(title="NexaPy Customer Support Automation")


@workflow(
    "customer-support",
    description="AI customer support workflow combining HTTP data lookup and AI response generation",
    schedule="0 8 * * *",
)
async def customer_support(ctx):
    customer_id = ctx.input.get("id", 1)
    user_question = ctx.input.get("question", "Help me with my account.")

    # 1. Fetch customer details via HTTP Action
    customer_res = await ctx.http.get(f"https://jsonplaceholder.typicode.com/users/{customer_id}")
    customer_data = customer_res.json() if customer_res.status_code == 200 else {}

    # 2. Process with AI Action
    prompt = f"""
    Customer Context:
    Name: {customer_data.get('name', 'Valued Customer')}
    Company: {customer_data.get('company', {}).get('name', 'N/A')}

    Customer Question:
    {user_question}

    Provide a helpful and friendly support response.
    """

    ai_response = await ctx.ai(prompt)

    return {
        "success": True,
        "customer": customer_data.get("name"),
        "answer": ai_response.text,
    }


# For development/demo, allow_anonymous=True is explicitly set.
# In production, provide api_key="your-api-key" or hmac_secret="your-hmac-secret".
app.mount_workflows(allow_anonymous=True)
