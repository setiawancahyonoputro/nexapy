from nexapy import NexaPy

app = NexaPy(title="Basic NexaPy Example App")


@app.get("/")
def home():
    return {"message": "Welcome to NexaPy Framework!"}


@app.get("/ping")
def ping():
    return {"ping": "pong"}
