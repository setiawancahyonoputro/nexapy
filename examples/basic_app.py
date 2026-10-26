from nexapy import NexaPy

app = NexaPy(title="Basic NexaPy Application")


@app.get("/")
def read_root():
    return {"message": "Welcome to NexaPy Framework!"}


@app.get("/health")
def health():
    return {"status": "ok", "app": "basic_app"}
